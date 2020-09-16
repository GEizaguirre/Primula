# Primula
## What is Primula
**Primula** is a serverless shuffle operator for general-purpose serverless frameworks. It is built upon the principles of scalability and transparency and it is designed for shuffle-like operations on routine data analysis pipelines.  

Primula provides several features for the automatization of shuffle-like workloads, mainly:
*  Automatic inference of the optimal number of parallel workers for the shuffle operation.
*  Load-balancing of workers through data sampling.
*  Eager mitigation of straggler functions.
*  Asynchronous MapReduce execution for performance.

## Architecture
Primula is based on IBM-PyWren (now [LitHop](https://github.com/lithops-cloud/lithops)), a serverless framework for massively parallel jobs. It currently supports IBM Cloud Functions as FaaS and IBM Cloud as remote storage service for data persistency and communication between functions.


## Acknowledgements
This work has been partially supported by the EUHorizon 2020 programme under grant agreement 825184 andby the Spanish Government (PID2019-106774RB-C22).
 
