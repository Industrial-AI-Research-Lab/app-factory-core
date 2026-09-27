# Добавление нового изолированного окружения (dev / staging / …) + CI/CD

**Блок:** Infrastructure / DevOps
**Приоритет:** Высокий
**Оценка:** 0.5–1 день на одно новое окружение (после того, как шаблон один раз отлажен)
**Зависимости:** Hetzner Cloud, DNS (Porkbun), GitHub repo с правами на Settings

---

## Use Case

Как разработчик, я хочу иметь возможность добавить новое изолированное окружение (dev, staging, preview-ветка и т.д.), где:
- Данные (MongoDB, Docker registry) полностью изолированы от других окружений;
- Наблюдаемость и LLM-прокси (Bifrost на production, Loki/Prometheus/Grafana/Jaeger на logging-хосте) переиспользуются, но с метками `environment=<имя>` и `host=<имя>` для разделения;
- Один и тот же Helm chart и один и тот же GitHub Actions workflow деплоят на любое окружение — разница только в секретах GitHub Environment.

Этот документ — пошаговый playbook, а не описание одного конкретного сервера. Если нужно поднять именно `dev` или именно `staging`, используйте таблицу «Конкретные значения» ниже как подстановку для переменных `${…}`.

---

## Что такое «новое окружение» в этом стеке

Одно окружение = **один Hetzner VM** + **один k3s-кластер** + **один namespace `AppFactory`** + **свой MongoDB и свой Docker registry внутри этого кластера**.

**Изолируется:**
- MongoDB (база в namespace `mongodb`, отдельный пароль, опубликована через NodePort `30017` для внешнего доступа Compass/mongosh)
- Docker registry (отдельный хост, свои htpasswd-креды)
- Helm release `AppFactory` и все pod'ы приложения
- DNS-записи (поддомены третьего уровня внутри `AppFactory.example.com`)

**Переиспользуется (shared, ничего не дублируется):**
- **Bifrost** (LLM-прокси) — `https://llm.AppFactory.example.com/v1`, stateless, на production-сервере
- **Loki** (логи) — `10.0.0.3:3100` (logging-хост), сегрегация по static-label `environment`
- **Prometheus** — `10.0.0.3:9090` (logging-хост), принимает remote_write от Alloy всех кластеров, сегрегация по external label `host`
- **Grafana** — `https://logs.AppFactory.example.com/` (logging-хост), те же дашборды, фильтр `$environment`
- **Jaeger** — `https://jaeger.AppFactory.example.com/` (logging-хост), OTLP HTTP `10.0.0.3:4318`, сегрегация по `OTEL_SERVICE_NAME=AppFactory-backend-<env>`
- **Porkbun DNS** — один аккаунт, один ClusterIssuer `letsencrypt-prod` на каждом кластере (API-ключ тот же)
- **SSH keypair `github-actions-AppFactory`** — один приватный ключ в GitHub Secrets, публичный раскладывается на каждый новый сервер

---

## Архитектура

```
                           GitHub Actions
                                │
                          workflow_dispatch
                          input: environment
                          (dev | staging | production | …)
                                │
            ┌───────────────────┼───────────────────┐
            ▼                   ▼                   ▼
      ┌──────────┐        ┌──────────┐        ┌──────────┐
      │   dev    │        │ staging  │        │ production│
      │ VM + k3s │        │ VM + k3s │        │ VM + k3s │
      │          │        │          │        │          │
      │ MongoDB  │        │ MongoDB  │        │ MongoDB  │◄── локально, изолировано
      │ Registry │        │ Registry │        │ Registry │
      │ Backend  │        │ Backend  │        │ Backend  │
      │ Frontend │        │ Frontend │        │ Frontend │
      └────┬─────┘        └────┬─────┘        └────┬─────┘
           │                   │                   │
           └───────────────────┼───────────────────┘
                               ▼
             ┌──────────────────────────────────┐
             │  Shared observability + LLM      │
             │                                  │
             │  Bifrost    (production host)    │
             │  Loki       (logging host)       │
             │  Prometheus (logging host)       │
             │  Grafana    (logging host)       │
             │  Jaeger     (logging host)       │
             └──────────────────────────────────┘
```

---

## Предварительные требования (сделать ВРУЧНУЮ до автоматизации)

Всё ниже — это условия, без которых скрипт установки не запустится. Разделены по тому, где именно вы это делаете.

### 1. Hetzner Cloud

- Создать VM в том же проекте `synaps` и той же Cloud Network (10.0.0.x).
- План: **CX33** (4 vCPU / 8 GB RAM / 80 GB SSD). CX23 (40 GB) не хватает на day 1 после установки k3s + registry + mongo.
- OS: **Ubuntu 24.04**.
- Присвоить VM постоянный приватный IP в сети `10.0.0.x`.
- Прикрепить public SSH key, которым вы как оператор будете заходить первый раз (ваш личный ключ, не GitHub Actions).

Записать себе: `${PUBLIC_IP}`, `${PRIVATE_IP}`, имя VM.

### 2. DNS (Porkbun, зона `example.com`)

Создать **A-записи** (TTL 300):

| Запись | Куда | Что это |
|---|---|---|
| `${WEB_HOST}` | `${PUBLIC_IP}` | Фронтенд |
| `${API_HOST}` | `${PUBLIC_IP}` | Бэкенд API |
| `registry.${ENV_NAME}.AppFactory.example.com` | `${PUBLIC_IP}` | Docker registry этого окружения |
| `*.${APPS_SUBDOMAIN}` | `${PUBLIC_IP}` | Wildcard для Deploy Agent приложений (если окружение будет хостить agent-apps) |

Wildcard-cert для `*.${APPS_SUBDOMAIN}` будет выпущен через DNS-01, поэтому wildcard A-запись работает без отдельной TXT-настройки.

### 3. GitHub Environment

В репозитории → **Settings → Environments → New environment → `${ENV_NAME}`**. Туда положить следующие секреты (полный список и какие уникальны / какие переиспользуются — см. раздел «Секреты GitHub Environment» ниже).

Минимально необходимые **до** первого запуска workflow'а:
- `SSH_HOST`, `SSH_KEY` (ключ переиспользуется из dev/prod)
- `REGISTRY_HOST`, `REGISTRY_USERNAME`, `REGISTRY_PASSWORD`
- `MONGODB_URI`, `MONGODB_DATABASE`
- `WEB_HOST`, `API_HOST`
- `PORKBUN_API_KEY`, `PORKBUN_SECRET_KEY`, `LETSENCRYPT_EMAIL`, `ROOT_DOMAIN=example.com`

### 4. Workflow dispatch input

В `.github/workflows/docker-build.yml` в `workflow_dispatch.inputs.environment.options` должно присутствовать имя нового окружения. Этот список — единственное место в коде, которое упоминает имена окружений; всё остальное берётся из GitHub Environment по имени.

---

## Конкретные значения для текущих окружений

Используется как подстановка для `${…}` переменных в этом документе.

| Переменная | dev | staging | production |
|---|---|---|---|
| `${ENV_NAME}` | `dev` | `staging` | `production` |
| `${PUBLIC_IP}` | `204.168.180.248` | `188.245.251.211` | `159.69.86.33` |
| `${PRIVATE_IP}` | `10.0.0.4` | `10.0.0.5` | `10.0.0.2` |
| `${WEB_HOST}` | `dev.AppFactory.example.com` | `staging.AppFactory.example.com` | `AppFactory.example.com` |
| `${API_HOST}` | `dev-api.AppFactory.example.com` | `staging-api.AppFactory.example.com` | `api.AppFactory.example.com` |
| `${REGISTRY_HOST}` | `registry.dev.AppFactory.example.com` | `registry.staging.AppFactory.example.com` | `registry.AppFactory.example.com` |
| `${APPS_SUBDOMAIN}` | `app.dev.AppFactory.example.com` | `app.staging.AppFactory.example.com` | `app.example.com` |
| `${OTEL_SERVICE_NAME}` | `AppFactory-backend-dev` | `AppFactory-backend-staging` | `AppFactory-backend-production` |
| `${LOKI_ENV_LABEL}` | `dev` | `staging` | `production` |
| `${MONGO_EXTERNAL}` | `204.168.180.248:30017` | `188.245.251.211:30017` | `159.69.86.33:30017` |

Внешний MongoDB-эндпоинт (`${PUBLIC_IP}:30017`) — это тот же in-cluster `mongodb` Service, поднятый как `type: NodePort`. Подключение — по тем же кредам `synaps / <MONGO_PASS>`, что и изнутри кластера.

---

## Секреты GitHub Environment

Три категории: **уникальные для окружения** (надо заполнять руками новыми значениями), **переиспользуемые из dev** (копировать verbatim) и **генерируемые при установке сервера** (сервер выдаст после шага установки).

### Уникальные для окружения

| Секрет | Источник значения |
|---|---|
| `SSH_HOST` | `${PUBLIC_IP}` |
| `WEB_HOST` | `${WEB_HOST}` |
| `API_HOST` | `${API_HOST}` |
| `REGISTRY_HOST` | `${REGISTRY_HOST}` |
| `DEPLOY_AGENT_APPS_DOMAIN` | `${APPS_SUBDOMAIN}` |
| `AppFactory_WILDCARD_DOMAIN` | `${APPS_SUBDOMAIN}` |
| `OTEL_SERVICE_NAME` | `${OTEL_SERVICE_NAME}` |
| `DEPLOY_AGENT_PROD_ENABLED` | `true` или `false` (см. примечание ниже) |

**Про `DEPLOY_AGENT_PROD_ENABLED`:** флаг, который в коде бэкенда гейтит agent-driven деплои в k8s. На production = `true`. На staging — рекомендуется `true`, чтобы прогонять полный пайплайн. На dev — исторически `false`, но если dev используется для тестирования Deploy Agent, тоже ставьте `true`.

### Переиспользуемые из dev (копировать как есть)

| Секрет | Примечание |
|---|---|
| `SSH_KEY` | Один и тот же ED25519 приватник `github-actions-AppFactory` для всех окружений. Публичный ключ раскладывается на сервер (см. шаг 6 скрипта) |
| `PORKBUN_API_KEY`, `PORKBUN_SECRET_KEY` | Один Porkbun-аккаунт на всю зону `example.com` |
| `LETSENCRYPT_EMAIL`, `ROOT_DOMAIN=example.com` | Одинаково |
| `BIFROST_URL=https://llm.AppFactory.example.com/v1`, `BIFROST_VK`, `BIFROST_FALLBACK_MODELS`, `USE_BIFROST=true` | Bifrost стоит на production-хосте и не дублируется |
| `OTEL_ENABLED=true`, `OTEL_ENDPOINT=http://10.0.0.3:4318/v1/traces` | Jaeger один, на logging-хосте, доступен через Hetzner private network |
| `OPENAI_API_KEY`, `INGRESS_CLASS=traefik`, `MONGODB_DATABASE=synaps`, `MONGODB_ENABLE_TRANSACTIONS`, `DEBUG`, `MAX_PRICE_PER_MILLION` | Значения применимы ко всем окружениям |

### Обязательно генерировать новые для каждого окружения

Не копировать из dev — это нарушит изоляцию/безопасность:

| Секрет | Как генерировать |
|---|---|
| `REGISTRY_USERNAME` | `ci` (так же, как везде) |
| `REGISTRY_PASSWORD` | `openssl rand -hex 16` (скрипт установки ниже создаст и положит в `/root/env-credentials/generated.txt`) |
| `MONGODB_URI` | `mongodb://synaps:<новый пароль>@mongodb.mongodb.svc.cluster.local:27017/synaps?authSource=admin` |
| `JWT_SECRET_KEY` | `openssl rand -hex 32` |
| `AppFactory_ROOT_PASSWORD` | `openssl rand -base64 24` |
| `DEPLOY_ADMIN_KEY` | `openssl rand -hex 32` |

---

## Шаг 1: Workflow dispatch — добавить имя окружения

В `.github/workflows/docker-build.yml`:

```yaml
on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Target environment'
        required: true
        default: 'dev'
        type: choice
        options:
          - dev
          - staging
          - production
          # + добавить новое имя сюда
```

На уровне job `build`:
```yaml
build:
  environment: ${{ github.event.inputs.environment || 'dev' }}
```

Все SSH-шаги берут хост/ключ/секреты из `${{ secrets.* }}` — GitHub автоматически подставит значения из выбранного Environment.

---

## Шаг 2: Скрипт установки на новом сервере (`setup-env-server.sh`)

Скрипт предполагает, что вы зашли на сервер как root или через sudo. Все переменные вверху — заполнить под конкретное окружение по таблице «Конкретные значения».

```bash
#!/bin/bash
set -euo pipefail

# --- PARAMETERS (заполнить под окружение) -----------------------------------
ENV_NAME="${ENV_NAME:?e.g. dev|staging}"
ROOT_DOMAIN="example.com"
PORKBUN_API_KEY="${PORKBUN_API_KEY:?}"
PORKBUN_SECRET_KEY="${PORKBUN_SECRET_KEY:?}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:?}"
REGISTRY_HOST="registry.${ENV_NAME}.AppFactory.${ROOT_DOMAIN}"
APPS_SUBDOMAIN="app.${ENV_NAME}.AppFactory.${ROOT_DOMAIN}"
LOKI_PUSH_URL="http://10.0.0.3:3100/loki/api/v1/push"
GITHUB_ACTIONS_PUBKEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHdd4kSL7XuDtU9mMSf1fts5coE8Y259EQ1/q9QFoXo/ github-actions-AppFactory"

# Генерируемые креды сохраняем, чтобы потом положить в GitHub Secrets:
mkdir -p /root/env-credentials
CREDS_FILE="/root/env-credentials/generated.txt"

MONGO_PASS="$(openssl rand -hex 16)"
REGISTRY_PASS="$(openssl rand -hex 16)"
REGISTRY_HTPASSWD="$(htpasswd -Bbn ci "$REGISTRY_PASS")"
REGISTRY_HTTP_SECRET="$(openssl rand -hex 32)"

cat > "$CREDS_FILE" <<EOF
# Generated $(date -Iseconds) for env=${ENV_NAME}
MONGO_PASS=${MONGO_PASS}
REGISTRY_USER=ci
REGISTRY_PASS=${REGISTRY_PASS}
REGISTRY_HTPASSWD=${REGISTRY_HTPASSWD}
REGISTRY_HTTP_SECRET=${REGISTRY_HTTP_SECRET}
MONGODB_URI=mongodb://synaps:${MONGO_PASS}@mongodb.mongodb.svc.cluster.local:27017/synaps?authSource=admin
EOF
chmod 600 "$CREDS_FILE"

# --- 0. Kernel: поднять inotify-лимиты ------------------------------
# Дефолт Ubuntu (max_user_instances=128) быстро выедается связкой
# k3s-server + containerd-shim + dind/dagger (все работают от uid=0
# на хостовом ядре и делят один бакет). Когда бакет упёрт, dagger-engine
# в dind падает в крэш-луп с "failed to create inotify fd", а бэкенд
# возвращает 500 на POST /api/projects. Подробнее — INOTIFY-LIMITS.md.
sysctl -w fs.inotify.max_user_instances=1024
sysctl -w fs.inotify.max_user_watches=524288
tee /etc/sysctl.d/99-k8s-inotify.conf > /dev/null <<'EOF'
# Raised to accommodate many containers sharing uid=0 on the host kernel.
# Default 128 is quickly exhausted by k3s + containerd-shim + dind/dagger,
# causing dagger-engine crash-loops ("failed to create inotify fd").
fs.inotify.max_user_instances=1024
fs.inotify.max_user_watches=524288
EOF

# --- 1. k3s ---------------------------------------------------------
curl -sfL https://get.k3s.io | sh -
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

# --- 2. Helm --------------------------------------------------------
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# --- 3. cert-manager + Porkbun webhook ------------------------------
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.yaml
kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=180s

helm repo add mdonoughe https://mdonoughe.github.io/porkbun-webhook
helm install porkbun-webhook mdonoughe/porkbun-webhook \
  -n cert-manager --version 0.1.5 \
  --set groupName="acme.${ROOT_DOMAIN}"

# КРИТИЧНО: chart не создаёт RBAC для своего SA — без этого DNS-01
# challenge застрянет в pending с "secrets porkbun-key is forbidden".
cat <<YAMLEOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: porkbun-webhook-secrets
  namespace: cert-manager
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list", "watch"]
YAMLEOF
cat <<YAMLEOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: porkbun-webhook-secrets
  namespace: cert-manager
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: porkbun-webhook-secrets
subjects:
- kind: ServiceAccount
  name: porkbun-webhook
  namespace: cert-manager
YAMLEOF

# Porkbun API credentials + ClusterIssuer создаются GitHub workflow'ом
# (шаг "Ensure Porkbun secret and ClusterIssuer"), но чтобы в этом же
# setup-скрипте выпустить registry-tls — можно создать их и тут:
kubectl -n cert-manager create secret generic porkbun-key \
  --from-literal=api-key="${PORKBUN_API_KEY}" \
  --from-literal=secret-key="${PORKBUN_SECRET_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

cat <<YAMLEOF | kubectl apply -f -
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt-prod
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    email: ${LETSENCRYPT_EMAIL}
    privateKeySecretRef:
      name: letsencrypt-prod-key
    solvers:
    - selector:
        dnsZones:
          - ${ROOT_DOMAIN}
      dns01:
        webhook:
          groupName: acme.${ROOT_DOMAIN}
          solverName: porkbun
          config:
            apiKeySecretRef:
              name: porkbun-key
              key: api-key
            secretKeySecretRef:
              name: porkbun-key
              key: secret-key
YAMLEOF

# --- 4. Namespaces --------------------------------------------------
for ns in AppFactory AppFactory-apps mongodb registry monitoring; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
done

# --- 5. MongoDB (in-cluster, PVC, NodePort для внешнего доступа) ----
kubectl -n mongodb create secret generic mongodb-creds \
  --from-literal=username=synaps \
  --from-literal=password="${MONGO_PASS}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Deploy + PVC — см. infra/k8s/mongodb/mongodb.yaml
# Service — NodePort 30017, чтобы можно было подключаться извне
# (Compass, Studio 3T, mongosh) к ${PUBLIC_IP}:30017. Аутентификация
# остаётся по логину/паролю synaps. Дефолт по всем окружениям.
cat <<YAMLEOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: mongodb
  namespace: mongodb
spec:
  type: NodePort
  selector:
    app: mongodb
  ports:
    - port: 27017
      targetPort: 27017
      nodePort: 30017
      protocol: TCP
YAMLEOF

# --- 6. Docker Registry (in-cluster, PVC, TLS от cert-manager) ------
kubectl -n registry create secret generic registry-htpasswd \
  --from-literal=HTPASSWD="${REGISTRY_HTPASSWD}" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n registry create secret generic registry-http \
  --from-literal=HTTP_SECRET="${REGISTRY_HTTP_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Deployment + Service + Ingress(tls: registry-tls) + Certificate → см. infra/k8s/registry/

# --- 7. CI user (для appleboy/ssh-action) ---------------------------
useradd -m -s /bin/bash ci || true
mkdir -p /home/ci/.kube /home/ci/bin /home/ci/.ssh
cp /etc/rancher/k3s/k3s.yaml /home/ci/.kube/config
chown -R ci:ci /home/ci/.kube /home/ci/.ssh /home/ci/bin
chmod 700 /home/ci/.ssh
chmod 600 /home/ci/.kube/config

echo "${GITHUB_ACTIONS_PUBKEY}" > /home/ci/.ssh/authorized_keys
chown ci:ci /home/ci/.ssh/authorized_keys
chmod 600 /home/ci/.ssh/authorized_keys

echo 'ci ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/ci
chmod 440 /etc/sudoers.d/ci

# КРИТИЧНО: KUBECONFIG должен быть в /etc/environment,
# потому что /usr/local/bin/kubectl → symlink к k3s, который
# без $KUBECONFIG читает /etc/rancher/k3s/k3s.yaml (root-only).
# appleboy/ssh-action = non-interactive non-login shell,
# поэтому .bashrc/.profile не подгружаются. /etc/environment
# подгружается через pam_env для любой SSH-сессии.
grep -q '^KUBECONFIG=' /etc/environment \
  || echo 'KUBECONFIG=/home/ci/.kube/config' >> /etc/environment
grep -q 'KUBECONFIG' /home/ci/.profile \
  || echo 'export KUBECONFIG=/home/ci/.kube/config' >> /home/ci/.profile
grep -q 'KUBECONFIG' /home/ci/.bashrc \
  || echo 'export KUBECONFIG=/home/ci/.kube/config' >> /home/ci/.bashrc

# Bootstrap-скрипт, который выполняет workflow на первом деплое
# (до того как helm release существует).
cat > /home/ci/bin/deploy.sh <<'DEPLOYSH'
#!/usr/bin/env bash
set -euo pipefail
export KUBECONFIG=/home/ci/.kube/config

: "${REGISTRY:?}"
: "${REGISTRY_USERNAME:?}"
: "${REGISTRY_PASSWORD:?}"
: "${MONGODB_URI:?}"
: "${MONGODB_DATABASE:?}"
: "${MONGODB_ENABLE_TRANSACTIONS:?}"
: "${OPENAI_API_KEY:?}"
DEBUG="${DEBUG:-false}"

kubectl create namespace AppFactory --dry-run=client -o yaml | kubectl apply -f -

kubectl -n AppFactory create secret docker-registry registry-cred \
  --docker-server="${REGISTRY}" \
  --docker-username="${REGISTRY_USERNAME}" \
  --docker-password="${REGISTRY_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n AppFactory create secret generic backend-secrets \
  --type=Opaque \
  --from-literal=MONGODB_URI="${MONGODB_URI}" \
  --from-literal=MONGODB_DATABASE="${MONGODB_DATABASE}" \
  --from-literal=MONGODB_ENABLE_TRANSACTIONS="${MONGODB_ENABLE_TRANSACTIONS}" \
  --from-literal=OPENAI_API_KEY="${OPENAI_API_KEY}" \
  --from-literal=DEBUG="${DEBUG}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "DEPLOY.SH: bootstrap secrets применены; helm upgrade выполнит сам workflow"
DEPLOYSH
chmod +x /home/ci/bin/deploy.sh
chown -R ci:ci /home/ci/bin

# --- 8. Alloy (логи + метрики + cert-пробы) --------------------------------
# ЕДИНЫЙ источник Alloy-конфига — infra/alloy/deploy-alloy.sh (chart 1.6.1, как на prod):
# логи -> Loki, метрики -> Prometheus remote_write (external label host=${ENV_NAME}),
# blackbox cert-пробы — только на production. НЕ дублируем конфиг здесь: инлайн-версия
# (logs-only, chart 1.7.0) разошлась бы с live, а её повторный прогон затёр бы метрики/пробы.
# Скрипт идемпотентен (helm upgrade --install + полный конфиг): повторный запуск сходится
# к тому же состоянию и ничего не ломает. Репозиторий private, curl'ом сырьё не тянется —
# скопируйте скрипт на сервер рядом:
#   scp infra/alloy/deploy-alloy.sh root@<этот-сервер>:/tmp/deploy-alloy.sh
# ENVIRONMENT ОБЯЗАТЕЛЕН: без него скрипт возьмёт default=production (коллизия host-метки + prod-пробы).
if [ -f /tmp/deploy-alloy.sh ]; then
  ENVIRONMENT="${ENV_NAME}" \
  LOKI_URL="${LOKI_PUSH_URL}" \
  PROM_REMOTE_WRITE_URL="http://10.0.0.3:9090/api/v1/write" \
    bash /tmp/deploy-alloy.sh
else
  echo "!! Alloy НЕ развёрнут: скопируйте infra/alloy/deploy-alloy.sh в /tmp/ и запустите:"
  echo "   ENVIRONMENT=${ENV_NAME} LOKI_URL=${LOKI_PUSH_URL} PROM_REMOTE_WRITE_URL=http://10.0.0.3:9090/api/v1/write bash /tmp/deploy-alloy.sh"
fi

echo "=== Done. Credentials saved at ${CREDS_FILE} ==="
echo "Скопируйте REGISTRY_PASS и MONGODB_URI в GitHub Environment '${ENV_NAME}'."
```

---

## Шаг 3: Helm values (опционально, per-env override)

Если нужны env-specific значения (реплики, ресурсы, extra env), создать `infra/helm/AppFactory/values-${ENV_NAME}.yaml`. Workflow автоматически подхватывает файл, если он есть (добавить одну строку в `helm upgrade`):

```yaml
VALUES_FILE=""
if [ -f "/home/ci/AppFactory-chart/values-${TARGET_ENV}.yaml" ]; then
  VALUES_FILE="-f /home/ci/AppFactory-chart/values-${TARGET_ENV}.yaml"
fi
helm upgrade AppFactory /home/ci/AppFactory-chart ${VALUES_FILE} ...
```

Для большинства окружений дефолтного `values.yaml` достаточно — все host'ы и домены приходят через `--set` из секретов.

---

## Шаг 4: Grafana (один раз — при добавлении первого нового окружения)

Чтобы дашборды умели фильтровать по окружению, добавить variable `environment` в дашборды:

```json
{
  "name": "environment",
  "type": "query",
  "datasource": "Loki",
  "query": "label_values(environment)",
  "current": { "text": "All", "value": "$__all" },
  "includeAll": true,
  "multi": true
}
```

В запросах панелей: `| environment=~"$environment"` (LogQL) или `{environment=~"$environment"}` (PromQL).

Один раз сделано — новые окружения автоматически появляются в селекторе.

---

## Подводные камни (реальный опыт)

Каждая проблема пропадёт, если выполнить `setup-env-server.sh` выше целиком. Если настраиваете руками — проверьте явно.

### 1. `/home/ci/bin/deploy.sh: No such file or directory` (exit 127)

**Симптом:** шаг `Deploy via SSH (restricted)` в workflow падает:
```
bash: line N: /home/ci/bin/deploy.sh: No such file or directory
```

**Причина:** workflow на первом деплое (до того как helm release существует) выполняет `deploy.sh` для bootstrap'а. На новом сервере его нет.

**Фикс:** создать `/home/ci/bin/deploy.sh` (см. шаг 7 скрипта), `chmod +x`, owner `ci:ci`.

### 2. `error loading config file "/etc/rancher/k3s/k3s.yaml": permission denied`

**Симптом:** любой шаг workflow, использующий `kubectl` напрямую, падает:
```
Unable to read /etc/rancher/k3s/k3s.yaml, please start server with --write-kubeconfig-mode
error: error loading config file "/etc/rancher/k3s/k3s.yaml": permission denied
```

**Причина:** `/usr/local/bin/kubectl` — это symlink на `/usr/local/bin/k3s`. `k3s kubectl` без `$KUBECONFIG` читает `/etc/rancher/k3s/k3s.yaml` (root-only). appleboy/ssh-action = non-interactive non-login shell → `.bashrc`/`.profile` не подгружаются.

**Фикс:** `KUBECONFIG` должен быть в `/etc/environment` (подгружается `pam_env` для любой SSH-сессии):
```bash
echo 'KUBECONFIG=/home/ci/.kube/config' | sudo tee -a /etc/environment
```

### 3. `dagger-engine` в dind падает в крэш-луп, `POST /api/projects` → 500

**Симптом:** в логах backend-pod'а:
```
ERROR sandbox.mcp_client_sdk | Failed to connect to container-use: Connection closed
ExceptionGroup: unhandled errors in a TaskGroup
```
В логах dind-сайдкара того же pod'а повторяющееся каждую секунду:
```
Error setting up exec command in container dagger-engine-v0.18.14: Container … is restarting
```
`docker inspect dagger-engine-v0.18.14` (внутри dind) показывает `exitCode=1, OOMKilled=false, restartCount=20+`.
В логах самого `dagger-engine`:
```
dnsmasq: failed to create inotify: No file descriptors available
level=warning msg="error from *cgroupsv2.Manager.EventChan" error="failed to create inotify fd"
dagger-engine: failed to create engine: failed to create network providers
```

**Причина:** на ноде `fs.inotify.max_user_instances = 128` (Ubuntu-дефолт) уже выбран связкой `k3s-server + containerd-shim + systemd`. Dagger-engine при старте не может выделить inotify-дескриптор. Это **не** OOM, **не** диск, **не** CPU — мониторинг ресурсов ничего не покажет.

**Фикс:** поднять лимит (шаг 0 в `setup-env-server.sh` уже это делает; если окружение поднималось до добавления шага — выполнить руками):
```bash
sudo sysctl -w fs.inotify.max_user_instances=1024
sudo sysctl -w fs.inotify.max_user_watches=524288
sudo tee /etc/sysctl.d/99-k8s-inotify.conf > /dev/null <<'EOF'
fs.inotify.max_user_instances=1024
fs.inotify.max_user_watches=524288
EOF
```
Вступает в силу мгновенно, ничего не перезапускать. Подробности — `INOTIFY-LIMITS.md`.

### 4. DNS-01 challenge в `pending` с `secrets "porkbun-key" is forbidden`

**Симптом:** `kubectl describe challenge ...`:
```
Reason: initialization error: get error for secret "cert-manager" "porkbun-key":
secrets "porkbun-key" is forbidden:
User "system:serviceaccount:cert-manager:porkbun-webhook" cannot get resource "secrets"
```

**Причина:** Helm chart `mdonoughe/porkbun-webhook` НЕ создаёт Role/RoleBinding для своего ServiceAccount.

**Фикс:** применить Role + RoleBinding из шага 3 `setup-env-server.sh`. После применения удалить зависшие challenges, чтобы cert-manager пересоздал их:
```bash
kubectl delete challenge --all -A
kubectl delete certificaterequest --all -A
```

---

## Чек-лист перед первым CI/CD-запуском

На сервере:
- [ ] `sysctl fs.inotify.max_user_instances` → `1024` (и файл `/etc/sysctl.d/99-k8s-inotify.conf` существует)
- [ ] `/home/ci/bin/deploy.sh` существует, executable, owner `ci:ci`
- [ ] `grep KUBECONFIG /etc/environment` → `/home/ci/.kube/config`
- [ ] `sudo -u ci kubectl get ns` работает без `--kubeconfig`
- [ ] `kubectl -n cert-manager get rolebinding porkbun-webhook-secrets` существует
- [ ] `kubectl get clusterissuer letsencrypt-prod` → `Ready=True`
- [ ] `kubectl get certificate -A` не содержит pending
- [ ] `/home/ci/.ssh/authorized_keys` содержит GitHub Actions pubkey
- [ ] `curl -u ci:<REGISTRY_PASS> https://${REGISTRY_HOST}/v2/_catalog` → 200
- [ ] `kubectl -n mongodb get svc mongodb -o jsonpath='{.spec.type}'` → `NodePort`, порт `30017` доступен снаружи

В GitHub:
- [ ] Environment `${ENV_NAME}` создан
- [ ] Все секреты из раздела «Секреты GitHub Environment» заполнены
- [ ] `docker-build.yml` → `workflow_dispatch.inputs.environment.options` содержит `${ENV_NAME}`

DNS:
- [ ] `dig +short ${WEB_HOST}` → `${PUBLIC_IP}`
- [ ] `dig +short ${API_HOST}` → `${PUBLIC_IP}`
- [ ] `dig +short ${REGISTRY_HOST}` → `${PUBLIC_IP}`

---

## Проверка после деплоя

### Автоматическая

```bash
# 1. Все поды запущены
kubectl get pods -n AppFactory
# backend, frontend — Running

# 2. MongoDB доступен (внутри кластера)
kubectl exec -n mongodb deploy/mongodb -- mongosh --eval "db.runCommand({ ping: 1 })"
# { ok: 1 }

# 2a. MongoDB доступен снаружи (NodePort 30017)
nc -vz ${PUBLIC_IP} 30017
# Connection to ${PUBLIC_IP} 30017 port [tcp/*] succeeded!
# + проверить подключение из Compass/mongosh: mongodb://synaps:<MONGO_PASS>@${PUBLIC_IP}:30017/synaps?authSource=admin

# 3. Сертификаты Ready
kubectl get certificate -A
# все Ready=True

# 4. Ingress работает
curl -sI https://${WEB_HOST} | head -1     # HTTP/2 200
curl -s https://${API_HOST}/api/health     # 200 OK

# 5. Registry
curl -u ci:${REGISTRY_PASS} https://${REGISTRY_HOST}/v2/_catalog
# {"repositories":[...]}
```

### Ручная

1. **CI/CD flow:** запустить workflow вручную с `environment=${ENV_NAME}` → деплой ушёл только на нужный хост.
2. **Логи:** Grafana → выбрать `environment=${ENV_NAME}` → видны логи с нового сервера.
3. **Изоляция данных:** создать запись в новом окружении → убедиться, что в MongoDB других окружений её нет.
4. **Bifrost/Jaeger:** проверить в Grafana/Jaeger, что трейсы и запросы к LLM помечены новым `OTEL_SERVICE_NAME` и не смешиваются с другими окружениями.

---

## Порядок выполнения для нового окружения

```
Этап 1: Предусловия (руки)
  ├─ Hetzner VM (CX33, Ubuntu 24.04, private IP в 10.0.0.x)
  ├─ DNS A-записи в Porkbun
  └─ GitHub Environment + секреты

Этап 2: Сервер (автоматизировано, setup-env-server.sh)
  ├─ sysctl: fs.inotify.max_user_instances=1024 (см. INOTIFY-LIMITS.md)
  ├─ k3s + Helm + cert-manager + Porkbun webhook (+ RBAC)
  ├─ ClusterIssuer letsencrypt-prod
  ├─ namespaces, MongoDB, Registry
  ├─ CI user + KUBECONFIG в /etc/environment + deploy.sh
  └─ Alloy через deploy-alloy.sh (копируется на сервер): логи + метрики + cert-пробы(prod)

Этап 3: Workflow
  ├─ Добавить ${ENV_NAME} в options
  └─ Запустить workflow_dispatch → environment=${ENV_NAME}

Этап 4: Проверка (чек-лист + smoke тесты)
```

---

## Что НЕ меняется при добавлении окружения

- **Production и другие существующие окружения** — ноль изменений.
- **Bifrost (production host), Loki / Prometheus / Grafana / Jaeger (logging host)** — только дашборды получают новое значение в фильтре `environment`, инфра не двигается.
- **Helm chart `infra/helm/AppFactory/`** — один и тот же, values приходят через `--set` из секретов.
- **Codebase приложения** — ничего.
- **SSH keypair GitHub Actions** — один и тот же на все окружения.
