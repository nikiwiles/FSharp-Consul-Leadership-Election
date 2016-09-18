(*** Leadership election with Hashicorp Consul in F# ***)

open System
open System.Text
open Consul

(** Unique identifier for this service **)
let serviceGuid = Guid.NewGuid().ToString()

(** Create a new ConsulClient - https://github.com/PlayFab/consuldotnet **)
let consulClient = new ConsulClient()

(** Nodes will contend over a common leadership key / value pair; the node that's able to obtain a lock on this key, becomes the leader **)
let leadershipKey = "leadershipkey"

(** The Consul session we'll use for setting and releasing locks **)
let session = consulClient.Session.Create(new SessionEntry()) |> Async.AwaitTask |> Async.RunSynchronously 

(** Checks to see if the current node is the leader **)
let getLeaderGuid () = consulClient.KV.Get(leadershipKey) |> Async.AwaitTask |> Async.RunSynchronously |> fun myKv -> myKv.Response.Value |> Encoding.Default.GetString

(** If there is a leadership election, all nodes attempt to lock 'leadershipKey'; if the response is true, the key has been successfully locked **)
let tryBecomeLeader () =  
    
    (** Console **)
    Console.WriteLine("Trying to become leader!")

    (** Handle for the leadership key **)
    let targetpair = new KVPair(leadershipKey)
    
    (** Set the session generated earlier **)
    targetpair.Session <- session.Response
    targetpair.Value <- System.Text.Encoding.ASCII.GetBytes(serviceGuid)

    (** Did we acquire the lock? **)
    let result = consulClient.KV.Acquire(targetpair) |> Async.AwaitTask |> Async.RunSynchronously 
    
    (** Print the result of the election **)
    match result.Response with 
    | true  -> Console.WriteLine("I succeeded :o)")
    | false -> Console.WriteLine("I failed :o(")  

(** Release the lock **)
let stepDown () = 
    
    (** Console **)
    Console.WriteLine("Stepping down as leader :o(")
    
    (** Handle for the leadership key **)
    let targetpair = new KVPair(leadershipKey)
    
    (** Set the session generated earlier **)
    targetpair.Session <- session.Response

    (** Did we acquire the lock? **)
    consulClient.KV.Release(targetpair) |> Async.AwaitTask |> Async.RunSynchronously |> ignore

(** All nodes run a continuous leadership loop (even the current leader, as it's lock may be released by an operator) **)
let watchLeadershipState () = async {
     
    (** Console **)
    Console.WriteLine("Watching for changes in leadership state ...")
    
    (** Our main loop **)
    let rec leadershipLoop (waitIndex: uint64) = 
        
        (** Many query style requests (e.g. KV.Get, Catalog.Services, Health.Service) have a method signature that accepts a QueryOptions class parameter **)
        let options = new QueryOptions()
        
        (** We use the waitIndex from the previous iteration, or zero, if this is the first iteration **)
        options.WaitIndex <- waitIndex
        
        (** This line returns after 1) the key has changed or 2) the timeout period has elapsed **)
        let result = async { let! result = consulClient.KV.Get(leadershipKey, options) |> Async.AwaitTask
                             return result 
                           } |> Async.RunSynchronously
        
        (** If we ever notice that the Session for the leadershipKey is blank, there is no leader, and we should try to obtain a lock  **)
        match result.Response.Session = null with
        | true  -> 
            
            (** We have a leadership election **)
            Console.WriteLine("Leadership election!")    

            (** Attempt to lock the leadership key **)
            tryBecomeLeader ()

        | false -> ()
        
        (** Each attempt to acquire the key should be separated by a timed wait. This is because Consul may be enforcing a lock-delay **)
        async { do! Async.Sleep (5000) } |> Async.RunSynchronously

        (** Iterate, using the latest WaitIndex **)
        leadershipLoop result.LastIndex

    (** Start our loop, setting the waitIndex to zero **)
    leadershipLoop ( Convert.ToUInt64 0 ) 
 }

(** If we're the leader, we're going to create a random failure in this service **)
let randomFailiure () = 
    
    (** If we're the leader, wait for a failure **)
    let rec failiureLoop () = 
         async { do! Async.Sleep (5000) } |> Async.RunSynchronously
         let leaderGuid = getLeaderGuid ()
         match leaderGuid = serviceGuid with 
         | false -> failiureLoop ()
         | true  -> let rnd = System.Random()
                    match rnd.Next (0, 100) > 80 with 
                    | true -> Console.WriteLine ("Uh oh, our service has failed!") ; raise <| System.Exception()
                    | false -> Console.WriteLine ("I survived as leader!") ; failiureLoop ()
    
    (** Start **)
    failiureLoop () 

try

    (** Starting up! **)
    Console.WriteLine("Starting node with GUID " + Guid.NewGuid().ToString())
  
    (** Initial leadership election **)
    tryBecomeLeader () 

    (** Launch node **)
    watchLeadershipState () |> Async.Start
  
    (** Kick off our random failure  thread **)
    randomFailiure ()

with | ex -> stepDown ()
   