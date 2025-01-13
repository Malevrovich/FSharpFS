namespace FSFS

type Undo = unit -> unit

type CompensativeResult<'success, 'failure> =
    | Success of 'success * Undo list
    | Failure of 'failure

module CompensativeResult =
    let bind f x =
        match x with
        | Failure e -> Failure e
        | Success(s1, undoList1) ->
            match f s1 with
            | Failure e ->
                undoList1 |> Seq.rev |> Seq.iter (fun undo -> undo ())
                Failure e
            | Success(s2, undoList2) -> Success(s2, undoList1 @ undoList2)

    let bindResult act revert x =
        match x with
        | Failure e -> Failure e
        | Success(s1, undoList1) ->
            match act s1 with
            | Error e ->
                undoList1 |> Seq.rev |> Seq.iter (fun undo -> undo ())
                Failure e
            | Ok(ok2) -> Success(ok2, undoList1 @ [ revert ])

    let bindUndoAndResult action x =
        match x with
        | Failure e -> Failure e
        | Success(s1, undoList1) ->
            match action s1 with
            | Error e ->
                undoList1 |> Seq.rev |> Seq.iter (fun undo -> undo ())
                Failure e
            | Ok(undoList, ok2) -> Success(ok2, undoList1 @ undoList)

    let toResult x =
        match x with
        | Failure e -> Error e
        | Success(s, undoList) -> Ok s

    let empty = Success((), [])
