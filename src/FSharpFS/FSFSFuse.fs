namespace FSFS

open Tmds.Fuse
open System.Text
open Tmds.Linux
open FSharp.Span.Utils

open FSFS.FileSystem
open FSFS.FileTree

type FSFSFileSystem(fileSystem: FileSystem) =
    inherit FuseFileSystemStringBase(Encoding.UTF8)

    let resultToErrorCode res =
        match res with
        | Ok(_) -> 0
        | Error(code) -> -code

    override this.SupportsMultiThreading: bool = true

    override this.Create(path: string, mode: mode_t, fi: byref<FuseFileInfo>) : int =
        fileSystem |> createFile path 0u 0u mode |> resultToErrorCode

    override this.Unlink(path: string) : int =
        fileSystem |> removeFile path |> resultToErrorCode

    override this.Truncate(path: string, length: uint64, fiRef: FuseFileInfoRef) : int =
        try
            fileSystem |> setFileSize path (uint length) false |> resultToErrorCode
        with e ->
            printfn "%A" e
            raise e


    override this.Read(path: string, off: uint64, span: System.Span<byte>, fi: byref<FuseFileInfo>) : int =
        try
            let res = read path (uint off) span fileSystem

            match res with
            | Ok res -> res
            | Error code -> -code
        with e ->
            printfn "%A" e
            raise e

    override this.Write(path: string, off: uint64, span: System.ReadOnlySpan<byte>, fi: byref<FuseFileInfo>) : int =
        try
            let res = write path (uint off) span fileSystem

            match res with
            | Ok res -> res
            | Error code -> -code
        with e ->
            printfn "%A" e
            raise e

    override this.MkDir(path: string, mode: mode_t) : int =
        fileSystem |> createDirectory path 0u 0u mode |> resultToErrorCode

    override this.RmDir(path: string) : int =
        fileSystem |> removeDirectory path |> resultToErrorCode

    override this.Chown(path: string, uid: uint32, gid: uint32, fiRef: FuseFileInfoRef) : int =
        fileSystem |> chown path uid gid |> resultToErrorCode

    override this.ChMod(path: string, mode: mode_t, fiRef: FuseFileInfoRef) : int =
        fileSystem |> chmod path mode |> resultToErrorCode

    override this.ReadDir
        (path: string, offset: uint64, flags: ReadDirFlags, content: DirectoryContent, fi: byref<FuseFileInfo>)
        : int =
        let readDirRes = fileSystem |> readDir path

        content.AddEntry(".")
        content.AddEntry("..")

        match readDirRes with
        | Ok dirsSeq ->
            let dirs = dirsSeq |> Array.ofSeq

            for i in 0 .. dirs.Length - 1 do
                content.AddEntry(dirs[i])

            0
        | Error code -> -code

    override this.GetAttr(path: string, stat: byref<stat>, fiRef: FuseFileInfoRef) : int =
        let nodeRes = fileSystem |> getNode path

        match nodeRes with
        | Ok node ->
            match node with
            | DirectoryNode(dirMD) ->
                stat.st_uid <- dirMD.Common.Uid |> uid_t.op_Implicit
                stat.st_gid <- dirMD.Common.Gid |> gid_t.op_Implicit
                stat.st_mode <- (LibC.S_IFDIR ||| (dirMD.Common.Mode))
                stat.st_nlink <- 2 |> uint64 |> nlink_t.op_Implicit
                0
            | FileNode(fileMD) ->
                stat.st_uid <- fileMD.Common.Uid |> uid_t.op_Implicit
                stat.st_gid <- fileMD.Common.Gid |> gid_t.op_Implicit
                stat.st_mode <- (LibC.S_IFREG ||| (fileMD.Common.Mode))
                stat.st_nlink <- 1 |> uint64 |> nlink_t.op_Implicit
                stat.st_size <- fileMD.Size |> int64 |> off_t.op_Implicit
                0
            | FillerNode(_) -> -LibC.ENOENT
        | Error code -> -code

    override this.Open(path: string, fi: byref<FuseFileInfo>) : int =
        fileSystem |> getNode path |> resultToErrorCode
