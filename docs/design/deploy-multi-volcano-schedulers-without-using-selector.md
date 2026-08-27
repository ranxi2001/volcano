# A easy way to deploy multi-volcano-scheduler without using selector

## Background

Currently the single scheduler can not satisfy the high throughput requirement in some scenarios. Besides the performance optimization against the single scheduler, another choice is to deploy multiple volcano schedulers to improve the overall scheduling throughput.

## Introduction
Previously we use label to divide the cluster nodes to multiple sections and each volcano scheduler is responsible for one section and then specify the schedulerName in the Pod Spec and submit it. It is inconvenient in some conditions especially for large clusters. This doc provides a another option for user to deploy multiple scheduler which needs less modification for workload and nodes.
A statefulset is used to deploy the volcano scheduler. The Job and Node are assigned to scheduler automatically based on the hash algorithm. 

！[multi-scheduler-deployment](images/multi-volcano-schedulers-without-using-selector.png) 

## How to deployment

### 1. Prepare the volcano scheduler yaml file. Here is a example for your reference.
```
kind: StatefulSet
apiVersion: apps/v1
metadata:
  name: volcano-scheduler
  namespace: volcano-system
  labels:
    app: volcano-scheduler
spec:
  replicas: 3
  selector:
    matchLabels:
      app: volcano-scheduler
  serviceName: "volcano-scheduler"
  template:
    metadata:
      labels:
        app: volcano-scheduler
    spec:
      serviceAccount: volcano-scheduler
      containers:
        - name: volcano-scheduler
          image: volcanosh/vc-scheduler:ae78900d21dce8522eb04b6817aac66c9abd01e2
          args:
            - --logtostderr
            - --scheduler-conf=/volcano.scheduler/volcano-scheduler.conf
            - --feature-gates=QueueAllocationReporting=true
            - --leader-elect=false
            - -v=3
            - 2>&1
          imagePullPolicy: "IfNotPresent"
          env:
            - name: MULTI_SCHEDULER_ENABLE
              value: "true"
            - name: SCHEDULER_NUM
              value: "3"
            - name: SCHEDULER_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: SCHEDULER_POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          volumeMounts:
            - name: scheduler-config
              mountPath: /volcano.scheduler
      volumes:
        - name: scheduler-config
          configMap:
            name: volcano-scheduler-configmap
apiVersion: v1
kind: Service
metadata:
  name: volcano-scheduler
  labels:
    app: volcano-scheduler
spec:
  ports:
  - port: 80
    name: volcano-scheduler
  clusterIP: None
  selector:
    app: volcano-scheduler
```            

Notes:
1. MULTI_SCHEDULER_ENABLE env is used to enable or disable  multi-scheduler.
2. SCHEDULER_NUM indicates the numbers of volcano schedulers which you are planning to launch.
3. Queue allocation reporting is an Alpha, fixed-membership protocol. The
   controller-manager must also enable `QueueAllocationReporting` and set
   `--queue-allocation-authoritative-ring-id=volcano-system/volcano-scheduler`
   and `--queue-allocation-authoritative-ring-members=3`.
   This declares that the ring is the complete accounting domain for every
   Queue. Do not enable it when Agent Scheduler, NodeShard, another scheduler
   ring, or another non-reporting scheduler shares those Queues.
4. Every scheduler replica must run, so leader election must be disabled for
   this StatefulSet. The reporting feature rejects leader election at startup.
5. Do not resize the ring while controller aggregation is active. Disable
   `QueueAllocationReporting` on the controller-manager, roll every scheduler
   member with the new `SCHEDULER_NUM`, update the authoritative member flag,
   and then re-enable controller aggregation. The controller keeps the prior
   total until one complete new generation has reported.
6. A `ResourceClaim` shared by jobs assigned to different ring members is not
   supported in this protocol version because claim deduplication is local to
   one reporter.
7. Reports do not expire by time. A permanently unavailable member can leave
   the aggregate stale-high until that StatefulSet ordinal recovers.
8. The Pod and PodGroup for a workload must resolve to the same consistent-hash
   ownership key. Volcano Jobs satisfy this by using the Job owner reference;
   other owner layouts are outside this protocol's accounting guarantee.

### 2. Deploy the statefulset
```
kubectl apply -f <volcano-statefulset.yaml>
```
