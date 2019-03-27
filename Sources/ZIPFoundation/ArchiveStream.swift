//
//  ArchiveStream.swift
//  ZIPFoundation
//
//  Created by Richard Lewin on 24/03/2019.
//

import Foundation

public enum ArchiveStreamError: Error {
    case noCurrentEntryError
    case unclosedEntryError
    case archiveClosedError
    case requiresZip64Error
    case compressionError
}

fileprivate protocol EntryStream {
    var uncompressedSize: Int { get }
    var compressedSize: Int { get }
    var checksum: CRC32 { get }
    
    func write(bytes: Data) throws
    
    func close() throws
}

fileprivate class UncompressedEntryStream: EntryStream {
    var uncompressedSize = 0
    var compressedSize = 0
    var checksum = CRC32(0)
    
    private let archive: Archive
    
    init(archive: Archive) {
        self.archive = archive
    }
    
    func write(bytes: Data) throws {
        checksum = bytes.crc32(checksum: checksum)
        uncompressedSize += bytes.count
        compressedSize += bytes.count
        
        _ = try Data.write(chunk: bytes, to: archive.archiveFile)
    }
    
    func close() {
        
    }
}

fileprivate class CompressedEntryStream: EntryStream {
    var uncompressedSize = 0
    var compressedSize = 0
    var checksum = CRC32(0)
    
    private let archive: Archive
    private let compressor: Compressor
    
    init(archive: Archive) throws {
        self.archive = archive
        
        if let compressor = Compressor(compress: true) {
            self.compressor = compressor
        } else {
            throw ArchiveStreamError.compressionError
        }
    }
    
    func write(bytes: Data) throws {
        checksum = bytes.crc32(checksum: checksum)
        uncompressedSize += bytes.count
        
        try compressor.process(bytes: bytes) { ptr, count in
            compressedSize += count
            do {
                _ = try Data.write(chunk: Data(bytesNoCopy: ptr, count: count, deallocator: .none), to: archive.archiveFile)
            } catch is CompressionError {
                throw ArchiveStreamError.compressionError
            }
        }
    }
    
    func close() throws {
        try compressor.finish(consumer: { ptr, count in
            compressedSize += count
            do {
                _ = try Data.write(chunk: Data(bytesNoCopy: ptr, count: count, deallocator: .none), to: archive.archiveFile)
            } catch is CompressionError {
                throw ArchiveStreamError.compressionError
            }
        })
    }
}

public class ArchiveStream {
    private class CurrentEntryContext {
        let path: String
        let modificationDateTime: (UInt16, UInt16)
        let permissions: UInt16
        let compressionMethod: CompressionMethod
        let localFileHeader: Archive.LocalFileHeader
        let localFileHeaderStart: Int
        let entryStream: EntryStream
        
        init(path: String, modificationDateTime: (UInt16, UInt16), permissions: UInt16, compressionMethod: CompressionMethod, localFileHeader: Archive.LocalFileHeader, localFileHeaderStart: Int, entryStream: EntryStream) {
            self.path = path
            self.modificationDateTime = modificationDateTime
            self.permissions = permissions
            self.compressionMethod = compressionMethod
            self.localFileHeader = localFileHeader
            self.localFileHeaderStart = localFileHeaderStart
            self.entryStream = entryStream
        }
    }
    
    private let archive: Archive
    private var closed = false
    private var currentEntryContext: CurrentEntryContext?
    private var centralDirectoryStructures = [Archive.CentralDirectoryStructure]()
    
    public init?(url: URL) {
        if let archive = Archive(url: url, accessMode: .create) {
            self.archive = archive
        } else {
            return nil
        }
    }
    
    public func beginEntry(with path: String, compressionMethod: CompressionMethod, modificationDate: Date = Date(), permissions: UInt16? = nil) throws {
        guard !closed else {
            throw ArchiveStreamError.archiveClosedError
        }
        
        if currentEntryContext != nil {
            try closeEntry()
        }
        
        let localFileHeaderStart = ftell(self.archive.archiveFile)
        let modDateTime = modificationDate.fileModificationDateTime
        let permissions = permissions ?? defaultFilePermissions
        
        let localFileHeader = try archive.writeLocalFileHeader(path: path, compressionMethod: compressionMethod, size: (0, 0), checksum: 0, modificationDateTime: modDateTime)
        
        let stream: EntryStream
        switch compressionMethod {
        case .deflate:
            stream = try CompressedEntryStream(archive: self.archive)
        case .none:
            stream = UncompressedEntryStream(archive: self.archive)
        }
        
        self.currentEntryContext = CurrentEntryContext(path: path, modificationDateTime: modDateTime, permissions: permissions, compressionMethod: compressionMethod, localFileHeader: localFileHeader, localFileHeaderStart: localFileHeaderStart, entryStream: stream)
    }
    
    public func write(bytes: Data) throws {
        guard let context = currentEntryContext else {
            throw ArchiveStreamError.noCurrentEntryError
        }
        
        try context.entryStream.write(bytes: bytes)
    }
    
    public func closeEntry() throws {
        guard let context = currentEntryContext else {
            throw ArchiveStreamError.noCurrentEntryError
        }
        
        try context.entryStream.close()
        
        guard context.entryStream.uncompressedSize <= UINT32_MAX && context.entryStream.compressedSize <= UINT32_MAX else {
            throw ArchiveStreamError.requiresZip64Error
        }
        
        let startOfCD = ftell(self.archive.archiveFile)
        fseek(self.archive.archiveFile, context.localFileHeaderStart, SEEK_SET)
        let localFileHeader = try archive.writeLocalFileHeader(path: context.path, compressionMethod: context.compressionMethod, size: (UInt32(context.entryStream.uncompressedSize), UInt32(context.entryStream.compressedSize)), checksum: context.entryStream.checksum, modificationDateTime: context.modificationDateTime)
        fseek(self.archive.archiveFile, startOfCD, SEEK_SET)
        
        let externalAttributes = FileManager.externalFileAttributesForEntry(of: .file, permissions: context.permissions)
        let offset = UInt32(context.localFileHeaderStart)
        let centralDir = Archive.CentralDirectoryStructure(localFileHeader: localFileHeader, fileAttributes: externalAttributes, relativeOffset: offset)
        self.centralDirectoryStructures.append(centralDir)
        
        self.currentEntryContext = nil
    }
    
    public func close() throws {
        guard !closed else {
            return
        }
        
        guard currentEntryContext == nil else {
            throw ArchiveStreamError.unclosedEntryError
        }
        
        guard centralDirectoryStructures.count <= UINT16_MAX else {
            throw ArchiveStreamError.requiresZip64Error
        }
        
        let startOfCD = ftell(self.archive.archiveFile)
        
        if startOfCD > UINT32_MAX {
            throw Archive.ArchiveError.invalidStartOfCentralDirectoryOffset
        }
        
        var sizeOfCentralDirectory = 0
        
        for centralDir in centralDirectoryStructures {
            sizeOfCentralDirectory += Archive.CentralDirectoryStructure.size
            sizeOfCentralDirectory += Int(centralDir.extraFieldLength)
            sizeOfCentralDirectory += Int(centralDir.fileNameLength)
            sizeOfCentralDirectory += Int(centralDir.fileCommentLength)
            
            _ = try Data.write(chunk: centralDir.data, to: self.archive.archiveFile)
        }
        
        let endOfCentralDirectoryRecord = Archive.EndOfCentralDirectoryRecord(record: self.archive.endOfCentralDirectoryRecord, numberOfEntriesOnDisk: UInt16(centralDirectoryStructures.count), numberOfEntriesInCentralDirectory: UInt16(centralDirectoryStructures.count), updatedSizeOfCentralDirectory: UInt32(sizeOfCentralDirectory), startOfCentralDirectory: UInt32(startOfCD))
        
        _ = try Data.write(chunk: endOfCentralDirectoryRecord.data, to: self.archive.archiveFile)
        
        fflush(self.archive.archiveFile)
        
        closed = true
    }
    
    public func addEntry(with path: String, compressionMethod: CompressionMethod, bytes: Data, modificationDate: Date = Date(), permissions: UInt16? = nil) throws {
        try beginEntry(with: path, compressionMethod: compressionMethod, modificationDate: modificationDate, permissions: permissions)
        try write(bytes: bytes)
        try closeEntry()
    }
}
