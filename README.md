* Add your code in `src/main/`
* Test your code with `src/tests/`
* Package your artifacts
* Modify dockerfile if needed
* Build and push docker image
* Deploy infrastructure with terraform
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch Spark app in cluster mode on Kubernetes Cluster
```
spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name sparkbasics \
    --conf spark.kubernetes.container.image=<spark-image> \
    ...
```
## Setup

1. Deploy Azure resources by running Terraform scripts:
```
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
```
2. Configure `kubectl` to connect to the AKS cluster:
```
az aks get-credentials -g <resource group name> -n <cluster name>
az acr create -n <registry name> -g <resource group name> --sku basic
az aks update -n <cluster name> -g <resource group name> --attach-acr <registry name>
```
6. Build and push Docker image to ACR:
```
az acr login -n <registry name>
docker build -f ./docker/Dockerfile -t <registry name>.azurecr.io/<image name>:<tag> .
docker push <registry name>.azurecr.io/<image name>:<tag>
```
7. Launch the Spark application in cluster mode on AKS. 
```
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit  --serviceaccount=default:spark --namespace=default
kubectl proxy

spark-submit \
    --master k8s://http://127.0.0.1:8001 \
    --deploy-mode cluster \
    --name sparkbasics 
    --class com.epam.spark.App \
    --num-executors 1 \
    --conf spark.driver.cores=1 \
    --conf spark.driver.memory=1g \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g  \
    --conf spark.kubernetes.executor.request.cores=600m \
    --conf spark.kubernetes.driver.request.cores=800m \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  \
    --conf spark.kubernetes.container.image=contreg01task.azurecr.io/sparkbasic:safr local:///opt/sparkbasics-1.0.0.jar
```
