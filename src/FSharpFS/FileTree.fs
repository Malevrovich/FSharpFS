module FSFS.FileTree

open Tmds.Linux
open FsToolkit.ErrorHandling

open FSFS.Persistence
open FSFS.RWLock

type StringWithAddr = BlockStorageAddr * string

let stringAddr (strAndMD: StringWithAddr) = fst strAndMD

type FileTreeNode =
    | FileNode of FileMetadata
    | DirectoryNode of DirectoryMetadata
    | FillerNode of string

and DirectoryMetadata =
    { Common: CommonMetadata
      Content: Map<string, FileTreeNode> }

and FileMetadata =
    { Common: CommonMetadata
      Size: uint
      SizeLock: RWLock
      Content: BlockStorageAddr list
      ExtensionAddrs: BlockStorageAddr list }

and CommonMetadata =
    { Name: StringWithAddr
      Addr: BlockStorageAddr
      Uid: uint
      Gid: uint
      Mode: mode_t }

let nodeCommon node =
    match node with
    | FileNode(fileMD) -> fileMD.Common
    | DirectoryNode(dirMD) -> dirMD.Common
    | FillerNode(str) -> (raise (System.ArgumentException()))

let nodeName node =
    match node with
    | FileNode(fileMD) -> snd fileMD.Common.Name
    | DirectoryNode(dirMD) -> snd dirMD.Common.Name
    | FillerNode(name) -> name

let private tryModifyNodeAtPath (path: string) action root =
    let rec tryRecModifyNodeAt path action root =
        match root with
        | FillerNode(name) -> Error LibC.ENOENT
        | FileNode(fileMD) -> Error LibC.ENOTDIR
        | DirectoryNode(dirMD) ->
            result {
                if not <| Seq.isEmpty path then
                    let! subtree =
                        dirMD.Content
                        |> Map.tryFind (path |> Seq.head)
                        |> Option.map Ok
                        |> Option.defaultValue (Error LibC.ENOENT)

                    let! (res, newSubtree) = tryRecModifyNodeAt (path |> Seq.tail) action subtree

                    return
                        res,
                        DirectoryNode(
                            { Common = dirMD.Common
                              Content = dirMD.Content |> Map.add (path |> Seq.head) newSubtree }
                        )
                else
                    return! action dirMD
            }

    let splittedPath = (path.Split "/")

    let trimmedPath =
        splittedPath |> Seq.skip 1 |> Seq.take (splittedPath.Length - 2) |> Array.ofSeq

    tryRecModifyNodeAt trimmedPath action root

let tryAddNodeAt (path: string) node root =
    let lastNodeAction (dirMD: DirectoryMetadata) =
        let name = nodeName node

        if dirMD.Content |> Map.containsKey name then
            printfn "Failed to add node %s at %A" name dirMD
            Error LibC.EEXIST
        else
            Ok
            <| (dirMD.Common.Addr,
                DirectoryNode(
                    { Common = dirMD.Common
                      Content = (dirMD.Content |> Map.add name node) }
                ))

    tryModifyNodeAtPath path lastNodeAction root

let trySetNodeAt (path: string) name setter root =
    let lastNodeAction (dirMD: DirectoryMetadata) =
        dirMD.Content
        |> Map.tryFind name
        |> Option.map (fun node ->
            Ok
            <| (dirMD.Common.Addr,
                DirectoryNode(
                    { Common = dirMD.Common
                      Content = (dirMD.Content |> Map.add name (setter node)) }
                )))
        |> Option.defaultValue (Error LibC.ENOENT)

    tryModifyNodeAtPath path lastNodeAction root

type UpdateResult =
    { Parent: DirectoryMetadata
      OldNode: FileTreeNode
      NewNode: FileTreeNode }

let tryUpdateNodeAt (path: string) name updater root =
    let lastNodeAction (dirMD: DirectoryMetadata) =
        dirMD.Content
        |> Map.tryFind name
        |> Result.requireSome LibC.ENOENT
        |> Result.bind (fun node ->
            updater node
            |> Result.map (fun (newNode) ->
                let newDirMD =
                    { Common = dirMD.Common
                      Content = (dirMD.Content |> Map.add name newNode) }

                ({ Parent = newDirMD
                   OldNode = node
                   NewNode = newNode },
                 DirectoryNode(newDirMD))))

    tryModifyNodeAtPath path lastNodeAction root

let tryRemoveNodeAt (path: string) name root =
    let lastNodeAction (dirMD: DirectoryMetadata) =
        if dirMD.Content |> Map.containsKey name then
            Ok
            <| (dirMD.Common.Addr,
                DirectoryNode(
                    { Common = dirMD.Common
                      Content = (dirMD.Content |> Map.remove name) }
                ))
        else
            Error LibC.ENOENT

    tryModifyNodeAtPath path lastNodeAction root

let tryGetNodeAt (path: string) root =
    let rec tryRecGetNodeAt path root =
        if path |> Seq.isEmpty then
            Ok root
        else
            match root with
            | FillerNode(name) -> Error LibC.ENOENT
            | FileNode(fileMD) -> Error LibC.ENOTDIR
            | DirectoryNode(dirMD) ->
                result {
                    let! subtree =
                        dirMD.Content
                        |> Map.tryFind (path |> Seq.head)
                        |> Option.map Ok
                        |> Option.defaultValue (Error LibC.ENOENT)

                    return! tryRecGetNodeAt (path |> Seq.tail) subtree
                }

    let splittedPath =
        if path = "/" then
            Seq.empty
        else
            (path.Split "/") |> Seq.skip 1

    tryRecGetNodeAt splittedPath root
