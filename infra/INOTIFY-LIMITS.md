# Inotify limits на k3s-нодах

**Блок:** Infrastructure / DevOps
**Приоритет:** Критический (без этой настройки dagger-engine в dind не запустится)
**Применимо:** все Hetzner-ноды, где крутится k3s + backend-под с dind/dagger (dev, staging, production)

---

## TL;DR

На новых Ubuntu/Debian-нодах `fs.inotify.max_user_instances` по умолчанию = **128**. Этот лимит общий для всех процессов одного UID на хостовом ядре (включая все контейнеры, где процесс работает от root). На k3s-ноде `k3s-server` + `systemd` + десяток `containerd-shim` выбирают бакет за несколько минут. Как только лимит упёрт, `dagger-engine` внутри `dind`-сайдкара не может выделить inotify-дескриптор при старте и падает в крэш-луп; создание проекта через `POST /api/projects` возвращает 500.

**Фикс на каждой k3s-ноде:** поднять лимит до 1024 и зафиксировать в `/etc/sysctl.d/`.

---

## Что такое inotify и почему лимит общий

`inotify` — механизм ядра Linux «наблюдай за файлом/директорией и сообщи об изменениях». Используется:

- `k3s-server` — наблюдение за `/etc/rancher/k3s/` и kubelet-мониторинг pod-ов
- `containerd-shim` — мониторинг cgroupsv2 EventChan для каждого контейнера
- `systemd` — наблюдение за unit-файлами
- `dnsmasq` внутри `dagger-engine` — следит за `/etc/hosts`
- Hot-reload конфигов, tail логов, file watchers в приложениях

Программа зовёт `inotify_init()`, ядро выдаёт один **инстанс** (один fd = одна «сессия наблюдения»). Внутри инстанса можно подписаться на много файлов (это отдельный лимит `max_user_watches`), но сам инстанс — это один канал.

**`fs.inotify.max_user_instances`** = сколько инстансов одновременно может держать один пользователь на всей машине. Ограничение считается по **реальному UID на хостовом ядре**, а не по PID-неймспейсу. Все контейнеры, работающие от root, делят один бакет uid=0 с самим хостом.

Когда бакет полон, следующий `inotify_init()` возвращает `ENFILE`. Ошибка в логах приложений выглядит по-разному и вводит в заблуждение:

- dagger-engine: `error from *cgroupsv2.Manager.EventChan error="failed to create inotify fd"`
- dnsmasq: `failed to create inotify: No file descriptors available`
- node_exporter / alloy / прочие: `too many open files`

Все три — одна и та же проблема: упёртый `max_user_instances`, а не file descriptors и не память.

---

## Почему дефолт 128 не подходит k8s-ноде

Дефолт выставлялся во времена, когда «один пользователь» = один человек за десктопом. На типичной k3s-ноде расход только системных процессов такой:

| процесс | расход инстансов |
|---|---|
| `k3s-server` | 40–100 (растёт с числом pod-ов) |
| `systemd` (PID 1) | 5–6 |
| `containerd-shim` | 2–3 на каждый запущенный контейнер |
| `NetworkManager`, `polkitd`, прочая обвязка | 1–2 каждый |

Уже на 15 pod-ов `containerd-shim`-ов одних насчитывается 30–45 инстансов. Вместе с k3s-server это уверенные 100+ из 128. Дальше любой новый контейнер (redeploy, запуск dind-движка, агентский pod) пробивает потолок.

---

## Симптомы, которые наблюдали

На dev (2026-04-21) и потенциально на staging:

1. `POST /api/projects` → 500 Internal Server Error.
2. В backend-логах:
   ```
   ERROR sandbox.mcp_client_sdk | Failed to connect to container-use: Connection closed
   ERROR sandbox.container_manager | Failed to create container: Cancelled via cancel scope …
   ExceptionGroup: unhandled errors in a TaskGroup (1 sub-exception)
   RuntimeError: Attempted to exit cancel scope in a different task than it was entered in
   ```
3. В dind-логах pod-а `AppFactory-backend`:
   ```
   Error setting up exec command in container dagger-engine-v0.18.14: Container … is restarting, wait until the container is running
   ```
   (повторяется каждую секунду)
4. `docker inspect dagger-engine-v0.18.14` внутри dind:
   ```
   state=restarting exitCode=1 OOMKilled=false restartCount=23+
   ```
5. Логи самого `dagger-engine`:
   ```
   dnsmasq: failed to create inotify: No file descriptors available
   dnsmasq exited: exit status 5
   level=warning msg="error from *cgroupsv2.Manager.EventChan" error="failed to create inotify fd"
   dagger-engine: failed to create engine: failed to create network providers
   ```

Ключевое — `OOMKilled=false`, диск и память на хосте в норме. Это сбивает с толку: обычный мониторинг ресурсов не покажет проблему.

---

## Диагностика

```bash
# 1. Текущий лимит
sysctl fs.inotify.max_user_instances fs.inotify.max_user_watches

# 2. Сколько занято по UID (на хосте, от root)
sudo bash -c '
declare -A cnt
for p in /proc/[0-9]*; do
  [ -r $p/status ] || continue
  uid=$(awk "/^Uid:/{print \$2; exit}" $p/status)
  n=$(ls -l $p/fd 2>/dev/null | grep -c "inotify")
  [ "$n" -gt 0 ] && cnt[$uid]=$(( ${cnt[$uid]:-0} + n ))
done
for u in "${!cnt[@]}"; do echo "uid=$u inotify_instances=${cnt[$u]}"; done
'

# 3. Топ-потребители
sudo bash -c '
for p in /proc/[0-9]*; do
  [ -r $p/status ] || continue
  uid=$(awk "/^Uid:/{print \$2; exit}" $p/status)
  n=$(ls -l $p/fd 2>/dev/null | grep -c "inotify")
  [ "$n" -gt 0 ] && echo "$n $uid ${p##*/} $(cat $p/comm 2>/dev/null)"
done
' | sort -rn | head -10
```

Если `uid=0 inotify_instances` близок к значению `max_user_instances` — вы в группе риска (даже если всё сейчас работает).

---

## Фикс

На каждой k3s-ноде (dev, staging, production) — от root:

```bash
# 1. Применить сразу, без рестартов
sudo sysctl -w fs.inotify.max_user_instances=1024
sudo sysctl -w fs.inotify.max_user_watches=524288

# 2. Зафиксировать, чтобы пережило ребут
sudo tee /etc/sysctl.d/99-k8s-inotify.conf > /dev/null <<'EOF'
# Raised to accommodate many containers sharing uid=0 on the host kernel.
# Default 128 is quickly exhausted by k3s + containerd-shim + dind/dagger,
# causing dagger-engine crash-loops ("failed to create inotify fd").
fs.inotify.max_user_instances=1024
fs.inotify.max_user_watches=524288
EOF

# 3. Проверить
sysctl fs.inotify.max_user_instances fs.inotify.max_user_watches
```

Изменение применяется мгновенно. Pod-ы перезапускать не нужно — `dagger-engine` в dind поднимется сам на следующей попытке рестарта (обычно в течение минуты).

**Безопасность:** inotify-инстансы стоят копейки по памяти (несколько KB каждый), ставить 1024 — стандартная рекомендация для k8s-нод (k3s docs, kubeadm docs, elasticsearch docs и т.д.). Обратный ход — убрать файл `/etc/sysctl.d/99-k8s-inotify.conf` и выполнить `sysctl --system`.

---

## Текущее состояние окружений (2026-04-21)

| окружение | лимит | фиксация | статус |
|---|---|---|---|
| production (`159.69.86.33`) | 1024 | `/etc/sysctl.d/99-inotify.conf` | ок |
| staging (`188.245.251.211`) | 1024 | `/etc/sysctl.d/99-k8s-inotify.conf` | ок (исправлено 2026-04-21) |
| dev (`204.168.180.248`) | 1024 | `/etc/sysctl.d/99-k8s-inotify.conf` | ок (исправлено 2026-04-21) |
| logging (`46.225.223.7`) | 128 | — | не критично (нет k3s/dagger) |

---

## Включено в новые окружения

Шаг с `sysctl` добавлен в `setup-env-server.sh` (см. `ADD-ENVIRONMENT.md`, раздел «Шаг 2») **до установки k3s**. Для всех новых окружений настройка выполняется автоматически — руками ничего делать не нужно.

---

## Почему это не `OOMKilled` и не утечка памяти

Частая ошибка при диагностике — списать крэш-луп на нехватку памяти, потому что сообщение `No file descriptors available` звучит как FD-лимит. Ни то, ни другое:

- `docker inspect` показывает `OOMKilled=false`
- `free -h` показывает несколько GB свободной памяти
- `ulimit -n` внутри dind — миллион (задаётся в entrypoint dagger)
- В `dmesg` нет `killed process` / `oom-killer`
- Ошибки стабильно повторяются при каждом рестарте движка — значит, детерминированная, а не race condition ресурсов

Единственный надёжный маркер — **сравнить `uid=0 inotify_instances` с `fs.inotify.max_user_instances`**. Если совпадают (или близки), проблема в этом.
