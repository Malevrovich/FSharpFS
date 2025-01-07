namespace FSFS

open Tmds.Fuse
open System.Text
open Tmds.Linux
open FSharp.Span.Utils
open type Tmds.Linux.LibC

type HelloFPFileSystem() =
    inherit FuseFileSystemBase()

    let helloFilePath = "/hello" |> Encoding.UTF8.GetBytes
    let helloFileContent = "Hello World!" |> Encoding.UTF8.GetBytes
    let rootPath = "/" |> Encoding.UTF8.GetBytes

    override this.SupportsMultiThreading: bool = true

    override this.GetAttr(path: System.ReadOnlySpan<byte>, stat: byref<Tmds.Linux.stat>, fiRef: FuseFileInfoRef) : int =
        match path with
        | root when Span.sequenceEqual path (span rootPath) ->
            stat.st_mode <- (S_IFDIR ||| (0b111_111_111 |> uint16 |> mode_t.op_Implicit)) // rwxr-xr-x
            stat.st_nlink <- 2 |> uint64 |> nlink_t.op_Implicit
            0
        | hello when Span.sequenceEqual path (span helloFilePath) ->
            stat.st_mode <- (S_IFREG ||| (0b100_100_100 |> uint16 |> mode_t.op_Implicit)) // r--r--r--
            stat.st_nlink <- 1 |> uint64 |> nlink_t.op_Implicit
            stat.st_size <- helloFileContent.Length |> off_t.op_Implicit
            0
        | _ -> -ENOENT

    override this.Open(path: System.ReadOnlySpan<byte>, fi: byref<FuseFileInfo>) : int =
        match path with
        | hello when Span.sequenceEqual path (span helloFilePath) ->
            if fi.flags &&& O_ACCMODE <> O_RDONLY then //
                -EACCES
            else
                0
        | _ -> -ENOENT

    override this.Read
        (path: System.ReadOnlySpan<byte>, offset: uint64, buffer: System.Span<byte>, fi: byref<FuseFileInfo>)
        : int =
        match path with
        | hello when Span.sequenceEqual path (span helloFilePath) ->
            if offset > uint64 helloFileContent.Length then
                0
            else
                let start = int offset
                let len = min buffer.Length (helloFileContent.Length - start)

                (span helloFileContent).Slice(start, len).CopyTo(buffer)
                len
        | _ -> -ENOENT

    override this.ReadDir
        (
            path: System.ReadOnlySpan<byte>,
            offset: uint64,
            flags: ReadDirFlags,
            content: DirectoryContent,
            fi: byref<FuseFileInfo>
        ) : int =
        match path with
        | root when Span.sequenceEqual path (span rootPath) ->
            let dirs = [ "."; ".."; "hello" ]

            for i in 0 .. dirs.Length - 1 do
                content.AddEntry(dirs[i])

            0
        | _ -> -ENOENT
