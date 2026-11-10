
## Prepare Minikube

### Build the docker images inside minikube

Build the images inside Minikube

```bash
eval $(minikube docker-env)

docker build -t as-demo:latest ~/projects/autosubmit/docker/demo
docker build --build-arg "AUTOSUBMIT_API_SOURCE=/api" --build-arg="PUBLIC_URL=/gui"\
  -t as-gui-demo:latest ~/projects/autosubmitreact-update/docker
```

The script above use the default names of the images. You can change and set them in the `values.yaml` as you wish.

### Install Nginx Ingress Controller

Install nginx Ingress Controller https://kubernetes.github.io/ingress-nginx/deploy/

```bash
helm upgrade --install ingress-nginx ingress-nginx \
  --repo https://kubernetes.github.io/ingress-nginx \
  --namespace ingress-nginx --create-namespace
```

Enable Ingress for testing

```bash
minikube addons enable ingress
```

## Install using helm

Install helm

```bash
helm install test-demo .
```


Clean up helm

```bash
helm uninstall test-demo
```

## Test locally

Forward nginx ingress controller port `80` to `localhost:8080`:

```bash
kubectl port-forward --namespace=ingress-nginx service/ingress-nginx-controller 8080:80
```