// ----------------------------------------------------------------------------
// F# TPL Dataflow MailboxProcessor implementation 
// (c) David Thomas 2012, Available under Apache 2.0 license.
// ----------------------------------------------------------------------------
namespace FSharp.Dataflow
open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Dataflow
#nowarn "40"

[<Sealed>]
type DataflowAgent<'Msg>(initial, ?cancelToken, ?dataflowOptions) =
    let cancellationToken = defaultArg cancelToken Async.DefaultCancellationToken
    let mutable started = false
    let errorEvent = new Event<System.Exception>()
    let options = defaultArg dataflowOptions <| DataflowBlockOptions()
    let incomingMessages = new BufferBlock<'Msg>(options)
    let mutable defaultTimeout = Timeout.Infinite
    
    member x.CurrentQueueLength() = incomingMessages.Count

    member x.DefaultTimeout
        with get() = defaultTimeout
        and set(value) = defaultTimeout <- value

    [<CLIEvent>]
    member this.Error = errorEvent.Publish

    member x.Start() =
        if started 
            then raise (new InvalidOperationException("Already Started."))
        else
            started <- true
            let comp = async { try do! initial x 
                                with error -> errorEvent.Trigger error }
            Async.Start(computation = comp, cancellationToken = cancellationToken)

    member x.Receive(?timeout) = 
        Async.AwaitTask <| incomingMessages.ReceiveAsync()

    member x.TryReceive(?timeout) = 
        let ts = TimeSpan.FromMilliseconds(float <| defaultArg timeout defaultTimeout)
        Async.AwaitTask <| incomingMessages.ReceiveAsync(ts)
                                .ContinueWith(fun (tt:Task<_>) -> 
                                                    if tt.IsCanceled || tt.IsFaulted then None
                                                    else Some tt.Result)

    member x.Post(item) = 
        let posted = incomingMessages.Post(item)
        if not posted then
            raise (InvalidOperationException("Incoming message buffer full."))

    member x.TryPostAndReply(replyChannelMsg, ?timeout) :'Reply option = 
        let timeout = defaultArg timeout defaultTimeout
        let resultCell = AsyncResultCell<_>()
        let msg = replyChannelMsg(new AsyncReplyChannel<_>(fun reply -> resultCell.RegisterResult(reply)))
        if incomingMessages.Post(msg) then
            resultCell.TryWaitResultSynchronously(timeout)
        else None

    member x.PostAndReply(replyChannelMsg, ?timeout) : 'Reply = 
        match x.TryPostAndReply(replyChannelMsg, ?timeout = timeout) with
        | None ->  raise (TimeoutException("PostAndReply timed out"))
        | Some result -> result

    member x.PostAndTryAsyncReply(replyChannelMsg, ?timeout): Async<'Reply option> = 
        let timeout = defaultArg timeout defaultTimeout
        let resultCell = AsyncResultCell<_>()
        let msg = replyChannelMsg(new AsyncReplyChannel<_>(fun reply -> resultCell.RegisterResult(reply)))
        let posted = incomingMessages.Post(msg)
        if posted then
            match timeout with
            |   Threading.Timeout.Infinite -> 
                    async { let! result = resultCell.AsyncWaitResult
                            return Some(result) }  
            |   _ ->
                    async { let! ok =  resultCell.GetWaitHandle(timeout)
                            let res = (if ok then Some(resultCell.GrabResult()) else None)
                            return res }
        else async{return None}

    member x.PostAndAsyncReply( replyChannelMsg, ?timeout) =                 
            let timeout = defaultArg timeout defaultTimeout
            match timeout with
            |   Threading.Timeout.Infinite -> 
                let resCell = AsyncResultCell<_>()
                let msg = replyChannelMsg (AsyncReplyChannel<_>(fun reply -> resCell.RegisterResult(reply) ))
                let posted = incomingMessages.Post(msg)
                if posted then
                    resCell.AsyncWaitResult  
                else
                    raise (InvalidOperationException("Incoming message buffer full."))
            |   _ ->            
                let asyncReply = x.PostAndTryAsyncReply(replyChannelMsg, timeout=timeout) 
                async { let! res = asyncReply
                        match res with 
                        | None ->  return! raise (TimeoutException("PostAndAsyncReply TimedOut"))
                        | Some res -> return res } 

    member x.TryScan((scanner: 'Msg -> Async<_> option), timeout): Async<_ option> = 
        let ts = TimeSpan.FromMilliseconds( float timeout)
        let rec loopformsg = async {
            let! msg = Async.AwaitTask <| incomingMessages.ReceiveAsync(ts)
                                            .ContinueWith(fun (tt:Task<_>) -> 
                                                if tt.IsCanceled || tt.IsFaulted then None
                                                else Some tt.Result)
            match msg with
            | Some m->  let res = scanner m
                        match res with
                        | None -> return! loopformsg
                        | Some res -> return! res 
            | None -> return None}                             
        loopformsg

    member x.Scan(scanner, timeout) =
        async { let! res = x.TryScan(scanner, timeout)
                match res with
                | None -> return raise(TimeoutException("Scan TimedOut"))
                | Some res -> return res }
                         
    static member Start(initial, ?cancellationToken, ?dataflowOptions) =
        let dfa = DataflowAgent<'Msg>(initial, ?cancelToken = cancellationToken, ?dataflowOptions = dataflowOptions)
        dfa.Start();dfa