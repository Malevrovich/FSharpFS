namespace FSFS

type Undo = unit -> unit

type CompensativeResult<'success, 'failure> =
    | Success of 'success * Undo list
    | Failure of 'failure

module CompensativeResult =
    let bind f x =
        match x with
        | Failure e -> Failure e
        | Success(valueLHS, undoLHS) ->
            match f valueLHS with
            | Failure e ->
                undoLHS |> Seq.rev |> Seq.iter (fun undo -> undo ())
                Failure e
            | Success(valueRHS, undoRHS) -> Success(valueRHS, undoLHS @ undoRHS)

    let map f x =
        match x with
        | Failure e -> Failure e
        | Success(valueLHS, undoLHS) ->
            let (valueRHS, undoRHS) = f valueLHS
            Success(valueRHS, undoLHS @ undoRHS)

    let iterRevertable f x =
        match x with
        | Failure e -> Failure e
        | Success(valueLHS, undoLHS) -> Success(valueLHS, undoLHS @ [ f valueLHS ])

    let bindRevertableResult act revert x =
        match x with
        | Failure e -> Failure e
        | Success(valueLHS, undoLHS) ->
            match act valueLHS with
            | Error e ->
                undoLHS |> Seq.rev |> Seq.iter (fun undo -> undo ())
                Failure e
            | Ok(valueRHS) -> Success(valueRHS, undoLHS @ [ revert ])

    let bindResult act x =
        match x with
        | Failure e -> Failure e
        | Success(valueLHS, undoLHS) ->
            match act valueLHS with
            | Error e ->
                undoLHS |> Seq.rev |> Seq.iter (fun undo -> undo ())
                Failure e
            | Ok(valueRHS) -> Success(valueRHS, undoLHS)

    let bindUndoAndResult action x =
        match x with
        | Failure e -> Failure e
        | Success(valueLHS, undoLHS) ->
            match action valueLHS with
            | Error e ->
                undoLHS |> Seq.rev |> Seq.iter (fun undo -> undo ())
                Failure e
            | Ok(valueRHS, undoList) -> Success(valueRHS, undoLHS @ undoList)

    let toResult x =
        match x with
        | Failure e -> Error e
        | Success(value, _) -> Ok value

    let empty = Success((), [])
