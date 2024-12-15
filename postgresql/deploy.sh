# sudo mkdir -p /mnt/data/postgresql
# sudo chmod 777 /mnt/data/postgresql
minikube ssh
sudo mkdir -p /data/postgresql
sudo chmod 777 /data/postgresql
exit

kubectl apply -f postgresql-on-k8s/postgres-configmap.yaml
kubectl get configmap

kubectl apply -f postgresql-on-k8s/psql-pv.yaml
kubectl apply -f postgresql-on-k8s/psql-claim.yaml
kubectl get pv
kubectl get pvc

kubectl apply -f postgresql-on-k8s/ps-deployment.yaml
kubectl get deployments
kubectl get pods

kubectl apply -f postgresql-on-k8s/ps-service.yaml
kubectl get svc

# minikube ip
# postgresql://ps_user:thiemcun@169@192.168.49.2:31714/ps_db
# psql -h 192.168.49.2 -p 31714 -U ps_user -d ps_db