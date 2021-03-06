# Cnvrg Capsule - Smart backup/restore engine for cnvrg control plane

### Motivation 

Cnvrg is an AI/MLOps end-to-end platform, and as a such it's should provide 
out of the box backups and restore solutions to ensure platform durability and reliability. 
The `cnvrg-capsule` service shipped within the cnvrg platform and is responsible for managing ( data and metadata) backups of platform critical, system workloads, such as PostgreSQL, Redis, ElasticSearch and others. 

### Concepts 
The goal of the backup systems is to minimise the downtime of the service in case of disaster. 
This means, it’s not enough to just create backup files and store them in a remote location. 
To reach the goal, backup/restore system must ensure that
* backups are valid, not corrupted, and they are actually able to restore the broken system 
* backups are stored in secure remote locations, which won’t be affected, even if the whole main system will goes down 
* backups are available for restore on demand 

### Cnvrg Capsule - overview
The cnvrg capsule service is designed as a simple, reliable backup and restore solution, which follows the main goal of the backup systems, it tries to minimise the service downtime as much as possible. 
The cnvrg capsule doesn’t manage any internal state, which makes this service completely stateless. Stateless allow to deploy and scale cnvrg capsule easily. 
The cnvrg capsule backups is depends on two components:
1. The S3 bucket - which is acts as backend storage for backups 
2. The `statefile.json` file, which is saved alongside with the actual backup and holds all the information about the actual backup, when it has been made, if the backup process has been successfully finished, etc. 

These two concepts (S3 bucket and `statefile.json`) make cnvrg capsule backups to be completely agnostic to any external systems, even when a potential user will completely loos the complete K8s cluster and all the storage disks, until the S3 bucket will be available, the data can br restore based on the actual backup file and the `statefile.json` that hold all the necessary metadata for the successful restore.


### Cnvrg Capsule - architecture 
Capsule has been designed as a standalone tool, but it provides 3 interfaces for backup management. 
The backup management includes operations like automatic backup discovery, 
configuration of rotation and period, etc. 
Probably the most efficient way to use a capsule is in conjunction
with `cnvrg-operator` and the `CnvrgApp` custom resource. 

The interfaces

* `CnvrgApp` scanner - capsule is capable to scan all the `CnvrgApp` instances in the cluster and build on top of its backup plan. (what to deploy, where to deploy, how often to deploy, etc..)  
* The HTTP API (allowing to list available backups per cnvrg cluster)
* The S3 watcher API (listing, and executing an backups per backup requests) 

Architecture schema: 

![capsule schema](./docs/schema.png)


Deployment options:

![1 to 1](./docs/1-to-1.png)

![many to 1](./docs/many-to-1.png)

![many to 1 multiple folders](./docs/many-to-1-multiple-folders.png)

![many to many](./docs/many-to-many.png)

![1 to 1 but many capsules](./docs/one-to-one-but-many.png)

### Install locally   

Download latest release from [capsule release page](https://github.com/AccessibleAI/cnvrg-capsule/releases) 
Once downloaded, copy the binary to your bin path, and optionally install completion
```shell
mv capsule-*-x86_64 /usr/local/bin/capsule
chmod +x /usr/local/bin/capsule
capsule completion bash > /usr/local/etc/bash_completion.d/capsule
```



### Usage

Capsule can be used in two mods, cli and daemon. 
The default setup for the capsule is to run it at least once as a daemon inside K8s cluster. 
Then, you can also install the capsule binary for running administrative tasks, such as list, describe or restore backups.

To start capsule in daemon mode
```shell
 capsule start
```

For the administrative tasks 
```shell
# list PostgreSQL backups 
capusle pg --list

# describe PostgreSQL backups
capusle pg --describe

# manually create PostgreSQL backup
capusle pg --create

# trigger PostgreSQL restore
capusle pg --restore

# downloading PostgreSQL backup
capusle pg --download

# manually delete existing  PostgreSQL backup
capusle pg --delete
```