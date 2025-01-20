module FSFS.FileSystem

open FSFS.FileTreePersistence
open FSFS.BitmapAllocatorPersistence
open FSFS.Persistence
open FSFS.CompensativeResult

open System
open System.Threading
open FsToolkit.ErrorHandling
open FSharp.Span.Utils
open Tmds.Linux

type BlockStorageInfo =
    { BlockSize: uint32
      BlockCount: uint32 }

type ObjectBlockStorage =
    { Info: BlockStorageInfo
      PersistableAllocator: PersistableBitmapAllocator
      ObjectIO: ObjectBlockStorageIOAdapterPool }

    interface IDisposable with
        member this.Dispose() : unit =
            (this.ObjectIO :> IDisposable).Dispose()

type DataBlockStorage =
    { Info: BlockStorageInfo
      PersistableAllocator: PersistableBitmapAllocator
      DataIO: DataBlockStorageIOAdapterPool }

    interface IDisposable with
        member this.Dispose() : unit = (this.DataIO :> IDisposable).Dispose()


type FileSystem =
    { mutable FileTree: FileTree.FileTreeNode
      MetadataStorage: ObjectBlockStorage
      StringStorage: ObjectBlockStorage
      DataStorage: DataBlockStorage }

    interface IDisposable with
        member this.Dispose() : unit =
            (this.MetadataStorage :> IDisposable).Dispose()
            (this.StringStorage :> IDisposable).Dispose()
            (this.DataStorage :> IDisposable).Dispose()

let mdAllocator fs =
    fs.MetadataStorage.PersistableAllocator.Allocator

let strAllocator fs =
    fs.StringStorage.PersistableAllocator.Allocator

let dataAllocator fs =
    fs.DataStorage.PersistableAllocator.Allocator

let mdAllocatorPersister fs =
    fs.MetadataStorage.PersistableAllocator.Persister

let strAllocatorPersister fs =
    fs.StringStorage.PersistableAllocator.Persister

let dataAllocatorPersister fs =
    fs.DataStorage.PersistableAllocator.Persister

let rec tryApplyTreeOperation operation fileTreeSelector (filesystem: FileSystem) =
    result {
        let tree = filesystem.FileTree
        let! res = operation tree
        let newTree = fileTreeSelector res

        if
            (LanguagePrimitives.PhysicalEquality (Interlocked.CompareExchange(&filesystem.FileTree, newTree, tree)) tree)
        then
            return res
        else
            return! tryApplyTreeOperation operation fileTreeSelector filesystem
    }

let splitLastSep (path: string) =
    let lastSep = path.LastIndexOf('/') + 1

    let dirPath = path.Substring(0, lastSep)
    let filename = path.Substring(lastSep)

    dirPath, filename

let unwrapFileNode node =
    match node with
    | FileTree.FileNode(fileMD) -> Ok fileMD
    | FileTree.DirectoryNode(_) -> Error LibC.EISDIR
    | FileTree.FillerNode(_) -> Error LibC.ENOENT

let unwrapDirNode node =
    match node with
    | FileTree.DirectoryNode(dirMD) -> Ok dirMD
    | FileTree.FileNode(_) -> Error LibC.ENOTDIR
    | FileTree.FillerNode(_) -> Error LibC.ENOENT


let createDirectory (path: string) (uid: uint) (gid: uint) (mode: mode_t) (filesystem: FileSystem) =
    // add filler node

    // allocate str
    // allocate node

    // persist str
    // persist str alloc
    // persist node
    // persist node alloc

    // add node

    let dirPath, filename = splitLastSep path

    let fillerNode = FileTree.FillerNode(filename)

    let addFillerOp = FileTree.tryAddNodeAt dirPath fillerNode
    let removeDir = FileTree.tryRemoveNodeAt dirPath filename

    CompensativeResult.empty
    |> bind (fun (_) -> Success(filesystem, []))
    |> bindRevertableResult (fun _ -> filesystem |> tryApplyTreeOperation addFillerOp snd) (fun () ->
        filesystem |> tryApplyTreeOperation removeDir snd |> ignore)
    |> bindUndoAndResult (fun (parentAddr, _) ->
        option {
            let! strAddrs = (strAllocator filesystem).Allocate(1u)
            let strAddr = strAddrs |> Seq.exactlyOne

            let revert = (fun () -> (strAllocator filesystem).Free([ strAddr ]))

            return Ok((parentAddr, { BlockId = strAddr }), [ revert ])
        }
        |> Option.defaultValue (Error LibC.ENOSPC))
    |> bindUndoAndResult (fun (parentAddr, strAddr) ->
        option {
            let! fileAddrs = (mdAllocator filesystem).Allocate(1u)
            let fileAddr = fileAddrs |> Seq.exactlyOne

            let revert = (fun () -> (mdAllocator filesystem).Free([ fileAddr ]))

            return Ok((parentAddr, strAddr, { BlockId = fileAddr }), [ revert ])
        }
        |> Option.defaultValue (Error LibC.ENOSPC))
    |> bindUndoAndResult (fun (parentAddr, strAddr, fileAddr) ->
        let dirMD =
            { FileTree.DirectoryMetadata.Common =
                { Name = (strAddr, filename)
                  Addr = fileAddr
                  Uid = uid
                  Gid = gid
                  Mode = mode }
              FileTree.DirectoryMetadata.Content = Map.empty }

        let dirNode = FileTree.DirectoryNode(dirMD)

        filesystem.StringStorage.ObjectIO.WriteObject filename strAddr
        (strAllocatorPersister filesystem).PersistBlockAllocation strAddr.BlockId

        let revertStrAlloc =
            (fun () -> (strAllocatorPersister filesystem).PersistBlockRelease strAddr.BlockId)

        let dirMDDTO = dirMD |> directoryMetadataToDTO parentAddr
        filesystem.MetadataStorage.ObjectIO.WriteObject dirMDDTO fileAddr
        (mdAllocatorPersister filesystem).PersistBlockAllocation fileAddr.BlockId

        let revertNodeAlloc =
            (fun () -> (mdAllocatorPersister filesystem).PersistBlockRelease fileAddr.BlockId)

        filesystem
        |> tryApplyTreeOperation (FileTree.trySetNodeAt dirPath filename (fun _ -> dirNode)) snd
        |> Result.map (fun res -> (res, [ revertStrAlloc; revertNodeAlloc ])))
    |> CompensativeResult.toResult
    |> Result.map ignore

let removeDirectory (path: string) (filesystem: FileSystem) =
    // replace node with filler

    // persist node release
    // persist str release
    // release node
    // release str

    // remove filler

    let dirPath, filename = splitLastSep path

    let updateOp node =
        node
        |> unwrapDirNode
        |> Result.bind (fun dirMD ->
            if dirMD.Content.IsEmpty then
                Ok <| FileTree.FillerNode filename
            else
                Error LibC.ENOTEMPTY)

    let replaceWithFillerOp = FileTree.tryUpdateNodeAt dirPath filename updateOp

    let removeFiller = FileTree.tryRemoveNodeAt dirPath filename

    filesystem
    |> tryApplyTreeOperation replaceWithFillerOp snd
    |> Result.bind (fun ({ FileTree.UpdateResult.OldNode = oldNode }, _) ->
        oldNode |> unwrapDirNode |> Result.mapError (fun _ -> LibC.EIO))
    |> Result.bind (fun dirMD ->
        (mdAllocatorPersister filesystem).PersistBlockRelease dirMD.Common.Addr.BlockId
        (strAllocatorPersister filesystem).PersistBlockRelease (fst dirMD.Common.Name).BlockId

        (mdAllocator filesystem).Free [ dirMD.Common.Addr.BlockId ]
        (strAllocator filesystem).Free [ (fst dirMD.Common.Name).BlockId ]

        filesystem |> tryApplyTreeOperation removeFiller snd)
    |> Result.map ignore

let createFile (path: string) (uid: uint) (gid: uint) (mode: mode_t) (filesystem: FileSystem) =
    // add filler node

    // allocate str
    // allocate node
    // allocate block

    // persist str
    // persist str alloc
    // persist block alloc
    // persist node
    // persist node alloc

    // add node
    let dirPath, filename = splitLastSep path

    let fillerNode = FileTree.FillerNode(filename)

    let addFillerOp = FileTree.tryAddNodeAt dirPath fillerNode
    let removeDir = FileTree.tryRemoveNodeAt dirPath filename

    CompensativeResult.empty
    |> bind (fun (_) -> Success(filesystem, []))
    |> bindRevertableResult (fun _ -> filesystem |> tryApplyTreeOperation addFillerOp snd) (fun () ->
        filesystem |> tryApplyTreeOperation removeDir snd |> ignore)
    |> bindUndoAndResult (fun (parentAddr, _) ->
        option {
            let! strAddrs = (strAllocator filesystem).Allocate(1u)
            let strAddr = strAddrs |> Seq.exactlyOne

            let revert = (fun () -> (strAllocator filesystem).Free([ strAddr ]))

            return Ok((parentAddr, { BlockId = strAddr }), [ revert ])
        }
        |> Option.defaultValue (Error LibC.ENOSPC))
    |> bindUndoAndResult (fun (parentAddr, strAddr) ->
        option {
            let! blockAddrs = (dataAllocator filesystem).Allocate(1u)
            let blockAddr = blockAddrs |> Seq.exactlyOne

            let revert = (fun () -> (dataAllocator filesystem).Free([ blockAddr ]))

            return Ok((parentAddr, strAddr, { BlockId = blockAddr }), [ revert ])
        }
        |> Option.defaultValue (Error LibC.ENOSPC))
    |> bindUndoAndResult (fun (parentAddr, strAddr, blockAddr) ->
        option {
            let! fileAddrs = (mdAllocator filesystem).Allocate(1u)
            let fileAddr = fileAddrs |> Seq.exactlyOne

            let revert = (fun () -> (mdAllocator filesystem).Free([ fileAddr ]))

            return Ok((parentAddr, strAddr, blockAddr, { BlockId = fileAddr }), [ revert ])
        }
        |> Option.defaultValue (Error LibC.ENOSPC))
    |> bindUndoAndResult (fun (parentAddr, strAddr, blockAddr, fileAddr) ->
        let fileMD: FileTree.FileMetadata =
            { Common =
                { Name = (strAddr, filename)
                  Addr = fileAddr
                  Uid = uid
                  Gid = gid
                  Mode = mode }
              Size = 0u
              SizeLock = RWLock.emptyLock
              Content = [ blockAddr ]
              ExtensionAddrs = [] }

        let fileNode = FileTree.FileNode(fileMD)

        filesystem.StringStorage.ObjectIO.WriteObject filename strAddr
        (strAllocatorPersister filesystem).PersistBlockAllocation strAddr.BlockId

        let revertStrAlloc =
            (fun () -> (strAllocatorPersister filesystem).PersistBlockRelease strAddr.BlockId)

        (dataAllocatorPersister filesystem).PersistBlockAllocation blockAddr.BlockId

        let revertBlockAlloc =
            (fun () -> (dataAllocatorPersister filesystem).PersistBlockRelease strAddr.BlockId)

        let fileMDDTO, _ = fileMD |> fileMetadataToDTOs parentAddr
        filesystem.MetadataStorage.ObjectIO.WriteObject fileMDDTO fileAddr
        (mdAllocatorPersister filesystem).PersistBlockAllocation fileAddr.BlockId

        let revertNodeAlloc =
            (fun () -> (mdAllocatorPersister filesystem).PersistBlockRelease fileAddr.BlockId)

        filesystem
        |> tryApplyTreeOperation (FileTree.trySetNodeAt dirPath filename (fun _ -> fileNode)) snd
        |> Result.map (fun res -> (res, [ revertStrAlloc; revertBlockAlloc; revertNodeAlloc ])))
    |> CompensativeResult.toResult
    |> Result.map ignore

let removeFile (path: string) (filesystem: FileSystem) =
    // replace node with filler

    // persist node release
    // persist node_blocks release
    // persist exts and exts_blocks release
    // persist str release
    // release node and exts
    // release blocks
    // release str

    // remove filler

    let dirPath, filename = splitLastSep path

    let updateOp node =
        node |> unwrapFileNode |> Result.map (fun node -> FileTree.FillerNode filename)

    let replaceWithFillerOp = FileTree.tryUpdateNodeAt dirPath filename updateOp

    let removeFiller = FileTree.tryRemoveNodeAt dirPath filename

    filesystem
    |> tryApplyTreeOperation replaceWithFillerOp snd
    |> Result.bind (fun ({ FileTree.UpdateResult.OldNode = oldNode }, _) ->
        oldNode |> unwrapFileNode |> Result.mapError (fun _ -> LibC.EIO))
    |> Result.bind (fun fileMD ->
        let blocksByNode = fileMD.Content |> Seq.chunkBySize maxBlocksInNode
        let fileMDBlocks = blocksByNode |> Seq.head
        let extsBlocks = blocksByNode |> Seq.tail |> Seq.zip fileMD.ExtensionAddrs

        (mdAllocatorPersister filesystem).PersistBlockRelease fileMD.Common.Addr.BlockId

        fileMDBlocks
        |> Seq.iter (fun blockAddr -> (dataAllocatorPersister filesystem).PersistBlockRelease blockAddr.BlockId)

        extsBlocks
        |> Seq.iter (fun (extAddr, blocks) ->
            (mdAllocatorPersister filesystem).PersistBlockRelease extAddr.BlockId

            blocks
            |> Seq.iter (fun blockAddr -> (dataAllocatorPersister filesystem).PersistBlockRelease blockAddr.BlockId))

        (strAllocatorPersister filesystem).PersistBlockRelease (fst fileMD.Common.Name).BlockId

        let mdBlocks =
            ([ fileMD.Common.Addr ] @ (fileMD.ExtensionAddrs)) |> List.map blockId

        (mdAllocator filesystem).Free(mdBlocks)
        (dataAllocator filesystem).Free(fileMD.Content |> List.map blockId)
        (strAllocator filesystem).Free [ blockId (fst fileMD.Common.Name) ]

        filesystem |> tryApplyTreeOperation removeFiller snd)
    |> Result.map ignore

let truncateFile parentAddr (fileMD: FileTree.FileMetadata) newSize (filesystem: FileSystem) =
    // persist size
    // persist ext
    // persist exts release
    // persist blocks release

    // release exts
    // release blocks

    let blocks = (newSize - 1u) / filesystem.DataStorage.Info.BlockSize + 1u
    let nodeIdx = ((blocks - 1u) / (uint maxBlocksInNode) + 1u) - 1u

    let newContent = fileMD.Content |> List.take (int blocks)
    let newExts = fileMD.ExtensionAddrs |> List.take (int nodeIdx)

    let newFileMD =
        { fileMD with
            Size = newSize
            Content = newContent
            ExtensionAddrs = newExts }

    let newFileMDDTO, extMDDTOs = newFileMD |> fileMetadataToDTOs parentAddr

    filesystem.MetadataStorage.ObjectIO.WriteObject newFileMDDTO fileMD.Common.Addr

    if nodeIdx <> 0u then
        let extAddr = newExts |> List.last

        extMDDTOs
        |> Seq.last
        |> (fun (extMDDTO) -> filesystem.MetadataStorage.ObjectIO.WriteObject extMDDTO extAddr)

    fileMD.ExtensionAddrs
    |> Seq.skip (int nodeIdx)
    |> Seq.iter (fun extAddr -> (mdAllocatorPersister filesystem).PersistBlockRelease extAddr.BlockId)

    fileMD.Content
    |> Seq.skip (int blocks)
    |> Seq.iter (fun blockAddr -> (dataAllocatorPersister filesystem).PersistBlockRelease blockAddr.BlockId)

    (mdAllocator filesystem).Free(fileMD.ExtensionAddrs |> List.skip (int nodeIdx) |> List.map blockId)
    (dataAllocator filesystem).Free(fileMD.Content |> List.skip (int blocks) |> List.map blockId)

    CompensativeResult.empty
    |> map (fun _ ->
        newFileMD, [ fun () -> printfn "Detected error during truncate! Reboot fs module, to get consistent state" ])

let updateFileSize parentAddr (fileMD: FileTree.FileMetadata) newSize (filesystem: FileSystem) =
    CompensativeResult.empty
    |> map (fun _ ->
        let newFileMD = { fileMD with Size = newSize }
        let newFileMDDTO, _ = newFileMD |> fileMetadataToDTOs parentAddr

        filesystem.MetadataStorage.ObjectIO.WriteObject newFileMDDTO fileMD.Common.Addr

        let revert =
            fun () ->
                (let oldFileMDDTO, _ = fileMD |> fileMetadataToDTOs parentAddr
                 filesystem.MetadataStorage.ObjectIO.WriteObject oldFileMDDTO fileMD.Common.Addr)

        newFileMD, [ revert ])

let extendFile parentAddr (fileMD: FileTree.FileMetadata) newSize (filesystem: FileSystem) =
    // allocate blocks
    // allocate exts

    // persist blocks reserve
    // persist exts
    // persist exts reserve
    // persist file

    let blocks = (newSize - 1u) / filesystem.DataStorage.Info.BlockSize + 1u
    let nodeIdx = ((blocks - 1u) / (uint maxBlocksInNode) + 1u) - 1u

    CompensativeResult.empty
    |> bindUndoAndResult (fun _ ->
        option {
            let! newBlocks = (dataAllocator filesystem).Allocate(blocks - uint fileMD.Content.Length)

            let revert = (fun () -> (dataAllocator filesystem).Free(newBlocks))

            let blockAddrs = newBlocks |> List.map (fun blockId -> { BlockId = blockId })

            return blockAddrs, [ revert ]
        }
        |> Result.requireSome LibC.ENOSPC)
    |> bindUndoAndResult (fun (newBlocks) ->
        option {
            if nodeIdx = 0u then
                return (newBlocks, []), []
            else
                let! extBlocks = (mdAllocator filesystem).Allocate(nodeIdx - uint fileMD.ExtensionAddrs.Length)

                let revert = (fun () -> (dataAllocator filesystem).Free(extBlocks))

                let extAddrs = extBlocks |> List.map (fun blockId -> { BlockId = blockId })

                return (newBlocks, extAddrs), [ revert ]
        }
        |> Result.requireSome LibC.ENOSPC)
    |> iterRevertable (fun (newBlocks, extAddrs) ->
        newBlocks
        |> Seq.iter (fun blockAddr -> (dataAllocatorPersister filesystem).PersistBlockAllocation blockAddr.BlockId)

        (fun () ->
            newBlocks
            |> Seq.iter (fun blockAddr -> (dataAllocatorPersister filesystem).PersistBlockRelease blockAddr.BlockId)))
    |> map (fun (newBlocks, extAddrs) ->
        let newContent = fileMD.Content @ newBlocks
        let newExts = fileMD.ExtensionAddrs @ extAddrs

        let newFileMD =
            { fileMD with
                Size = newSize
                Content = newContent
                ExtensionAddrs = newExts }

        let newFileMDDTO, extMDDTOs = newFileMD |> fileMetadataToDTOs parentAddr

        if not fileMD.ExtensionAddrs.IsEmpty then
            newExts
            |> Seq.zip extMDDTOs
            |> Seq.skip (fileMD.ExtensionAddrs.Length - 1)
            |> Seq.iter (fun (extMDDTO, addr) -> filesystem.MetadataStorage.ObjectIO.WriteObject extMDDTO addr)

        let revert =
            fun () ->
                (let _, oldExtMDDTOs = fileMD |> fileMetadataToDTOs parentAddr

                 if not fileMD.ExtensionAddrs.IsEmpty then
                     let lastAddr = fileMD.ExtensionAddrs |> List.last

                     oldExtMDDTOs
                     |> Seq.last
                     |> (fun extMDDTO -> filesystem.MetadataStorage.ObjectIO.WriteObject extMDDTO lastAddr))

        (newFileMD, newFileMDDTO, extAddrs), [ revert ])
    |> iterRevertable (fun (newFileMD, newBlocks, extAddrs) ->
        extAddrs
        |> Seq.iter (fun extAddr -> (mdAllocatorPersister filesystem).PersistBlockAllocation extAddr.BlockId)

        (fun () ->
            extAddrs
            |> Seq.iter (fun extAddr -> (mdAllocatorPersister filesystem).PersistBlockRelease extAddr.BlockId)))
    |> iterRevertable (fun (newFileMD, newFileMDDTO, _) ->
        filesystem.MetadataStorage.ObjectIO.WriteObject newFileMDDTO fileMD.Common.Addr

        fun () ->
            (let oldFileMDDTO, _ = fileMD |> fileMetadataToDTOs parentAddr
             filesystem.MetadataStorage.ObjectIO.WriteObject oldFileMDDTO fileMD.Common.Addr))
    |> map (fun (newFileMD, _, _) -> newFileMD, [])

let setFileSize (path: string) (newSize: uint) (filesystem: FileSystem) =
    let dirName, filename = splitLastSep path

    let lockFileSize node =
        node
        |> unwrapFileNode
        |> Result.map (fun fileMD ->
            option {
                let! newSizeLock = fileMD.SizeLock |> RWLock.tryAcquireWriteLock
                return FileTree.FileNode { fileMD with SizeLock = newSizeLock }
            })
        |> Result.bind (Result.requireSome LibC.EBUSY)

    let unlockFileWithNewMD newMD node =
        node
        |> unwrapFileNode
        |> Result.mapError (fun _ -> LibC.EIO)
        |> Result.map (fun fileMD ->
            let md = newMD |> Option.defaultValue fileMD

            FileTree.FileNode
                { md with
                    SizeLock = fileMD.SizeLock |> RWLock.releaseWriteLock })

    let lockFileSizeOp = FileTree.tryUpdateNodeAt dirName filename lockFileSize

    let unlockFileSizeOp =
        FileTree.tryUpdateNodeAt dirName filename (unlockFileWithNewMD None)

    let unlockFileWithNewMDOop newMD =
        FileTree.tryUpdateNodeAt dirName filename (unlockFileWithNewMD <| Some newMD)

    CompensativeResult.empty
    |> bindRevertableResult //
        (fun _ -> filesystem |> tryApplyTreeOperation lockFileSizeOp snd)
        (fun _ -> filesystem |> tryApplyTreeOperation unlockFileSizeOp snd |> ignore)
    |> bindResult //
        (fun
            ({ FileTree.UpdateResult.NewNode = lockedNode
               FileTree.UpdateResult.Parent = parent },
             _) ->
            let parentAddr = parent.Common.Addr

            lockedNode
            |> unwrapFileNode
            |> Result.mapError (fun _ -> LibC.EIO)
            |> Result.map (fun fileMD -> fileMD, parentAddr))
    |> bind (fun (fileMD, parentAddr) ->
        let blocks = int ((newSize - 1u) / filesystem.DataStorage.Info.BlockSize + 1u)

        if blocks > fileMD.Content.Length then
            filesystem |> extendFile parentAddr fileMD newSize
        elif blocks = fileMD.Content.Length then
            filesystem |> updateFileSize parentAddr fileMD newSize
        else
            filesystem |> truncateFile parentAddr fileMD newSize)
    |> bindResult (fun fileMD -> filesystem |> tryApplyTreeOperation (unlockFileWithNewMDOop fileMD) snd)
    |> toResult
    |> Result.ignore

let rec private splitSpanByBlockSeq blockSize content (offset: int) spanLen =
    seq {
        let size = min spanLen (blockSize - offset)
        let rem = spanLen - size
        let addr = content |> List.head

        yield (addr, offset, size)

        if rem <> 0 then
            yield! (splitSpanByBlockSeq blockSize (content |> List.tail) 0 rem)
    }

let read (path: string) (offset: uint) (buffer: byte span) (filesystem: FileSystem) =
    let fileMDRes =
        filesystem.FileTree
        |> FileTree.tryGetNodeAt path
        |> Result.bind (fun node ->
            match node with
            | FileTree.DirectoryNode(_) -> Error LibC.EISDIR
            | FileTree.FillerNode(_) -> Error LibC.ENOENT
            | FileTree.FileNode(fileMD) -> Ok(fileMD))

    // unable to use Result.Bind due to span
    match fileMDRes with
    | Error(code) -> Error code
    | Ok(fileMD) ->
        let blockSize = filesystem.DataStorage.Info.BlockSize

        let size = int (min (fileMD.Size - offset) (uint buffer.Length))
        let dst = buffer[0..size]

        let readSeq =
            splitSpanByBlockSeq
                (int blockSize)
                (fileMD.Content |> List.skip (int (offset / blockSize)))
                (int (offset % blockSize))
                dst.Length

        for (addr, readOffset, readSize) in readSeq do
            let readDst = dst[0..readSize]

            filesystem.DataStorage.DataIO.ReadData(addr, readDst, readOffset)

        Ok size

let write (path: string) (offset: uint) (buffer: byte readonlyspan) (filesystem: FileSystem) =
    let fileMDRes =
        filesystem.FileTree
        |> FileTree.tryGetNodeAt path
        |> Result.bind (fun node ->
            match node with
            | FileTree.DirectoryNode(_) -> Error LibC.EISDIR
            | FileTree.FillerNode(_) -> Error LibC.ENOENT
            | FileTree.FileNode(fileMD) -> Ok(fileMD))

    // unable to use Result.Bind due to span
    match fileMDRes with
    | Error(code) -> Error code
    | Ok(fileMD) ->
        let blockSize = filesystem.DataStorage.Info.BlockSize

        let size = int (min (fileMD.Size - offset) (uint buffer.Length))
        let dst = buffer[0..size]

        let writeSeq =
            splitSpanByBlockSeq
                (int blockSize)
                (fileMD.Content |> List.skip (int (offset / blockSize)))
                (int (offset % blockSize))
                dst.Length

        for (addr, writeOffset, writeSize) in writeSeq do
            let writeDst = dst[0..writeSize]

            filesystem.DataStorage.DataIO.WriteData(addr, writeDst, writeOffset)

        Ok size

let chown (path: string) (uid: uint) (gid: uint) (filesystem: FileSystem) =
    let dirPath, filename = splitLastSep path

    let updateOwner node =
        match node with
        | FileTree.FileNode(fileMD) ->
            Ok
            <| FileTree.FileNode
                { fileMD with
                    Common.Uid = uid
                    Common.Gid = gid }
        | FileTree.DirectoryNode(dirMD) ->
            Ok
            <| FileTree.DirectoryNode
                { dirMD with
                    Common.Uid = uid
                    Common.Gid = gid }
        | FileTree.FillerNode(_) -> Error LibC.ENOENT

    let updateOp = FileTree.tryUpdateNodeAt dirPath filename updateOwner

    filesystem |> tryApplyTreeOperation updateOp snd |> Result.map ignore

let chmod (path: string) (mode: mode_t) (filesystem: FileSystem) =
    let dirPath, filename = splitLastSep path

    let updateOwner node =
        match node with
        | FileTree.FileNode(fileMD) -> Ok <| FileTree.FileNode { fileMD with Common.Mode = mode }
        | FileTree.DirectoryNode(dirMD) -> Ok <| FileTree.DirectoryNode { dirMD with Common.Mode = mode }
        | FileTree.FillerNode(_) -> Error LibC.ENOENT

    let updateOp = FileTree.tryUpdateNodeAt dirPath filename updateOwner

    filesystem |> tryApplyTreeOperation updateOp snd |> Result.map ignore


let readDir (path: string) (filesystem: FileSystem) =
    filesystem.FileTree
    |> FileTree.tryGetNodeAt path
    |> Result.bind (fun node ->
        match node with
        | FileTree.DirectoryNode(dirMD) -> dirMD.Content |> Map.keys |> Ok
        | FileTree.FileNode(_) -> Error LibC.ENOTDIR
        | FileTree.FillerNode(_) -> Error LibC.ENOTDIR)

let getNode (path: string) (filesystem: FileSystem) =
    filesystem.FileTree |> FileTree.tryGetNodeAt path
