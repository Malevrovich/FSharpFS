namespace FSFS

(* 
Note that it's only RWLock logic, without implementation

Usage example:
let mutable lock = emptyLock

let tryLock = 
    let lockBefore = lock

    tryAcquireReadLock lock
    |> Option.bind (
        fun lockAfter -> 
            if (LanguagePrimitives.PhysicalEquality (Interlocked.CompareExchange(&lock, lockAfter, lockBefore)) lockBefore)
            then
                return Some lockAfter
            else
                return None
    ) 
*)

type RWLock = { WriteLocked: bool; ReadLocked: int }

module RWLock =
    let emptyLock = { WriteLocked = false; ReadLocked = 0 }

    let tryAcquireReadLock lock =
        if lock.WriteLocked then
            None
        else
            Some
                { lock with
                    ReadLocked = lock.ReadLocked + 1 }

    let releaseReadLock lock =
        { lock with
            ReadLocked = lock.ReadLocked - 1 }

    let tryUpgradeLock lock =
        if lock.WriteLocked || (lock.ReadLocked <> 1) then
            None
        else
            Some
                { lock with
                    WriteLocked = true
                    ReadLocked = 0 }

    let tryAcquireWriteLock lock =
        if lock.WriteLocked || (lock.ReadLocked <> 0) then
            None
        else
            Some { lock with WriteLocked = true }

    let releaseWriteLock lock = { lock with WriteLocked = false }
