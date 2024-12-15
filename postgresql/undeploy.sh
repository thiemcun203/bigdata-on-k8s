#  Delete deployment first (to release the PVC)
kubectl delete deployment postgres

# Delete the PVC
kubectl delete pvc postgres-volume-claim

# Delete the PV
kubectl delete pv postgres-volume

# Delete the configmap
kubectl delete configmap postgres-secret