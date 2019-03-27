//
//  Compressor.swift
//  ZIPFoundation
//
//  Created by Richard Lewin on 27/03/2019.
//

import Foundation
import Compression

enum CompressionError: Error {
    case processError
    case compressorFinishedError
}

class Compressor {
    
    static let bufferSize = 16384
    
    private let streamPointer: UnsafeMutablePointer<compression_stream>
    private let dstBufferPointer: UnsafeMutablePointer<UInt8>
    
    private var status: compression_status
    private(set) var finished = false
    
    init?(compress: Bool) {
        streamPointer = UnsafeMutablePointer<compression_stream>.allocate(capacity: 1)
        let op = compress ? COMPRESSION_STREAM_ENCODE : COMPRESSION_STREAM_DECODE
        status = compression_stream_init(streamPointer, op, COMPRESSION_ZLIB)
        guard status != COMPRESSION_STATUS_ERROR else {
            return nil
        }
        
        dstBufferPointer = UnsafeMutablePointer<UInt8>.allocate(capacity: Compressor.bufferSize)
        streamPointer.pointee.dst_ptr = dstBufferPointer
        streamPointer.pointee.dst_size = Compressor.bufferSize
    }
    
    deinit {
        compression_stream_destroy(streamPointer)
        streamPointer.deallocate()
        dstBufferPointer.deallocate()
    }
    
    func process(bytes: Data, consumer: (UnsafeMutablePointer<UInt8>, Int) throws -> Void) throws {
        try process(bytes: bytes, finish: false, consumer: consumer)
    }
    
    func finish(consumer: (UnsafeMutablePointer<UInt8>, Int) throws -> Void) throws {
        if !finished {
            try process(bytes: Data(), finish: true, consumer: consumer)
        }
    }
    
    func process(bytes: Data, finish: Bool, consumer: (UnsafeMutablePointer<UInt8>, Int) throws -> Void) throws {
        guard !finished else {
            throw CompressionError.compressorFinishedError
        }
        
        let flags = finish ? Int32(COMPRESSION_STREAM_FINALIZE.rawValue) : 0
        
        try bytes.withUnsafeBytes { body in
            streamPointer.pointee.src_ptr = body.baseAddress!.assumingMemoryBound(to: UInt8.self)
            streamPointer.pointee.src_size = bytes.count
            
            repeat {
                if streamPointer.pointee.dst_size == 0 {
                    try consumer(dstBufferPointer, Compressor.bufferSize)
                    
                    streamPointer.pointee.dst_ptr = dstBufferPointer
                    streamPointer.pointee.dst_size = Compressor.bufferSize
                }
                
                status = compression_stream_process(streamPointer, flags)
                
                switch status {
                case COMPRESSION_STATUS_OK:
                    break
                    
                case COMPRESSION_STATUS_END:
                    if streamPointer.pointee.dst_ptr > dstBufferPointer {
                        try consumer(dstBufferPointer, streamPointer.pointee.dst_ptr - dstBufferPointer)
                    }
                    finished = true
                    
                case COMPRESSION_STATUS_ERROR:
                    throw CompressionError.processError
                    
                default:
                    break
                }
                
            } while status != COMPRESSION_STATUS_END && streamPointer.pointee.dst_size == 0
        }
    }
}
