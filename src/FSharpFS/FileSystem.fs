module FSFS.FileSystem

open FSFS.FileTree
open FSFS.FileTreePersistence
open FSFS.BitmapAllocatorPersistence
open FSFS.Persistence
open FSFS.CompensativeResult

open System
open System.Threading
open FsToolkit.ErrorHandling
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
    { mutable FileTree: FileTreeNode
      MetadataStorage: ObjectBlockStorage
      StringStorage: ObjectBlockStorage
      DataAllocator: DataBlockStorage }

    interface IDisposable with
        member this.Dispose() : unit =
            (this.MetadataStorage :> IDisposable).Dispose()
            (this.StringStorage :> IDisposable).Dispose()
            (this.DataAllocator :> IDisposable).Dispose()

let mdAllocator fs =
    fs.MetadataStorage.PersistableAllocator.Allocator

let strAllocator fs =
    fs.StringStorage.PersistableAllocator.Allocator

let mdAllocatorPersister fs =
    fs.MetadataStorage.PersistableAllocator.Persister

let strAllocatorPersister fs =
    fs.StringStorage.PersistableAllocator.Persister

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

    let fillerNode = FillerNode(filename)

    let addFillerOp = tryAddNodeAt dirPath fillerNode
    let removeDir = tryRemoveNodeAt dirPath filename

    let revertDir =
        (fun () -> filesystem |> tryApplyTreeOperation removeDir snd |> ignore)

    CompensativeResult.empty
    |> bind (fun (_) -> Success(filesystem, []))
    |> bindResult (fun _ -> filesystem |> tryApplyTreeOperation addFillerOp snd) revertDir
    |> bindUndoAndResult (fun (parentAddr, _) ->
        option {
            let! strAddrs = (strAllocator filesystem).Allocate(1u)
            let strAddr = strAddrs |> Seq.exactlyOne

            let revert = (fun () -> (strAllocator filesystem).Free([ strAddr ]))

            return Ok([ revert ], (parentAddr, { BlockId = strAddr }))
        }
        |> Option.defaultValue (Error LibC.ENOSPC))
    |> bindUndoAndResult (fun (parentAddr, strAddr) ->
        option {
            let! fileAddrs = (mdAllocator filesystem).Allocate(1u)
            let fileAddr = fileAddrs |> Seq.exactlyOne

            let revert = (fun () -> (mdAllocator filesystem).Free([ fileAddr ]))

            return Ok([ revert ], (parentAddr, strAddr, { BlockId = fileAddr }))
        }
        |> Option.defaultValue (Error LibC.ENOSPC))
    |> bindUndoAndResult (fun (parentAddr, strAddr, fileAddr) ->
        let dirMD =
            { Common =
                { Name = (strAddr, filename)
                  Addr = fileAddr
                  Uid = uid
                  Gid = gid
                  Mode = mode }
              Content = Map.empty }

        let dirNode = DirectoryNode(dirMD)

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
        |> tryApplyTreeOperation (trySetNodeAt dirPath filename (fun _ -> dirNode)) snd
        |> Result.map (fun res -> ([ revertStrAlloc; revertNodeAlloc ], res)))
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
        match node with
        | DirectoryNode(dirMD) when (snd dirMD.Common.Name) = filename ->
            if dirMD.Content.IsEmpty then
                Ok <| FillerNode filename
            else
                Error LibC.ENOTEMPTY
        | DirectoryNode(_) -> Error LibC.ENOENT
        | FillerNode(_) -> Error LibC.ENOENT
        | FileNode(_) -> Error LibC.ENOTDIR

    let replaceWithFillerOp = tryUpdateNodeAt dirPath filename updateOp

    let removeFiller = tryRemoveNodeAt dirPath filename

    filesystem
    |> tryApplyTreeOperation replaceWithFillerOp snd
    |> Result.bind (fun (prevNode, _) ->
        match prevNode with
        | DirectoryNode(dirMD) -> Ok dirMD
        | _ -> Error LibC.ENOTDIR)
    |> Result.bind (fun dirMD ->
        (mdAllocatorPersister filesystem).PersistBlockRelease dirMD.Common.Addr.BlockId
        (strAllocatorPersister filesystem).PersistBlockRelease (fst dirMD.Common.Name).BlockId

        (mdAllocator filesystem).Free [ dirMD.Common.Addr.BlockId ]
        (strAllocator filesystem).Free [ (fst dirMD.Common.Name).BlockId ]

        filesystem |> tryApplyTreeOperation removeFiller snd)
    |> Result.map ignore

// let createFile (path: string) (uid: uint) (gid: uint) (mode: mode_t) (filesystem: FileSystem) =

let chown (path: string) (uid: uint) (gid: uint) (filesystem: FileSystem) =
    let dirPath, filename = splitLastSep path

    let updateOwner node =
        match node with
        | FileNode(fileMD) ->
            Ok
            <| FileNode
                { fileMD with
                    Common.Uid = uid
                    Common.Gid = gid }
        | DirectoryNode(dirMD) ->
            Ok
            <| DirectoryNode
                { dirMD with
                    Common.Uid = uid
                    Common.Gid = gid }
        | FillerNode(_) -> Error LibC.ENOENT

    let updateOp = tryUpdateNodeAt dirPath filename updateOwner

    filesystem |> tryApplyTreeOperation updateOp snd |> Result.map ignore

let chmod (path: string) (mode: mode_t) (filesystem: FileSystem) =
    let dirPath, filename = splitLastSep path

    let updateOwner node =
        match node with
        | FileNode(fileMD) -> Ok <| FileNode { fileMD with Common.Mode = mode }
        | DirectoryNode(dirMD) -> Ok <| DirectoryNode { dirMD with Common.Mode = mode }
        | FillerNode(_) -> Error LibC.ENOENT

    let updateOp = tryUpdateNodeAt dirPath filename updateOwner

    filesystem |> tryApplyTreeOperation updateOp snd |> Result.map ignore


let readDir (path: string) (filesystem: FileSystem) =
    filesystem.FileTree
    |> tryGetNodeAt path
    |> Result.bind (fun node ->
        match node with
        | DirectoryNode(dirMD) -> dirMD.Content |> Map.keys |> Ok
        | FileNode(_) -> Error LibC.ENOTDIR
        | FillerNode(_) -> Error LibC.ENOTDIR)

let getNode (path: string) (filesystem: FileSystem) =
    filesystem.FileTree |> tryGetNodeAt path
