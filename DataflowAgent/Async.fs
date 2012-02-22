// ----------------------------------------------------------------------------
// F# TPL Dataflow MailboxProcessor implementation 
// (c) David Thomas 2012, Available under Apache 2.0 license.
// ----------------------------------------------------------------------------
namespace FSharp.Dataflow
open System
open System.Threading
open System.Threading.Tasks

[<Sealed>]
type AsyncReplyChannel<'Reply> internal (replyf : 'Reply -> unit) =
    member x.Reply(reply) = replyf(reply)

[<Sealed>]
type internal AsyncResultCell<'a>() =
    let source = new TaskCompletionSource<'a>()

    member x.RegisterResult result = source.SetResult(result)

    member x.AsyncWaitResult =
        Async.FromContinuations(fun (cont,_,_) -> 
            let apply = fun (task:Task<_>) -> cont (task.Result)
            source.Task.ContinueWith(apply) |> ignore)

    member x.GetWaitHandle(timeout:int) =
        async { let waithandle = source.Task.Wait(timeout)
                return waithandle }

    member x.GrabResult() = source.Task.Result

    member x.TryWaitResultSynchronously(timeout:int) = 
        //early completion check
        if source.Task.IsCompleted then 
            Some source.Task.Result
        //now force a wait for the task to complete
        else 
            if source.Task.Wait(timeout) then 
                Some source.Task.Result
            else None