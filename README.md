# isucon-template-v2

## 練習環境の準備
https://github.com/matsuu/aws-isucon

上記リポジトリに記載の AMI からインスタンスを起動。
以下のユーザーデータを設定すると便利。
```bash
#!/bin/bash -ex

# スペース区切り
USERS="harsssh"

for user in $USERS; do
    sudo -u isucon mkdir -p /home/isucon/.ssh
    sudo -u isucon chmod 700 /home/isucon/.ssh
    curl "https://github.com/${user}.keys" | sudo -u isucon tee -a /home/isucon/.ssh/authorized_keys
    sudo -u isucon chmod 600 /home/isucon/.ssh/authorized_keys
done
```

ローカルの `~/.ssh/config` に以下を設定。
```
Host isu
    Hostname xxx.xxx.xxx.xxx
    User isucon
    ForwardAgent yes
    # For pprotein
    LocalForward 9000 localhost:9000

Host bench
    Hostname yyy.yyy.yyy.yyy 
    User isucon
    ForwardAgent yes
```

## 初回計測までの手順

### ソースコードの push まで

名前は何でもいいが、短い方が楽
```bash
git clone $REPO_URL $REPO_DIR
cd $REPO_DIR
```

go-task をインストール
```bash
assets/install_go_task.sh
```

"Frequently changed" の部分を編集してから, `SetupTasks.yml` を実行.
```bash
task -t SetupTasks.yml all
source ~/.bashrc  # alias を読み込む
```

pprotein を起動
```bash
task enable -- pprotein
```

Go が古すぎる場合は新しくインストール
```bash
task -t SetupTasks.yml install-go
source ~/.bashrc  # PATH を読み込む
```

gitignore を設定し, pushする
```bash
cd app
cat << 'EOF' >> .gitignore
node
python
# ...
EOF

git add -A && git commit -m "init" && git push
```

### ログの設定

nginx
```nginx configuration
# /etc/nginx/nginx.conf
log_format ltsv "time:$time_local"
                "\thost:$remote_addr"
                "\tforwardedfor:$http_x_forwarded_for"
                "\treq:$request"
                "\tstatus:$status"
                "\tmethod:$request_method"
                "\turi:$request_uri"
                "\tsize:$body_bytes_sent"
                "\treferer:$http_referer"
                "\tua:$http_user_agent"
                "\treqtime:$request_time"
                "\tcache:$upstream_http_x_cache"
                "\truntime:$upstream_http_x_runtime"
                "\tapptime:$upstream_response_time"
                "\tvhost:$host";
access_log /var/log/nginx/access.log ltsv;
```

mysql
```
# /etc/mysql/mysql.conf.d/mysqld.cnf
[mysqld]
slow_query_log = 1
slow_query_log_file = /var/log/mysql/mysql-slow.log
long_query_time = 0
log-queries-not-using-indexes
```

app (echo の場合)
```go
// main.go
import (
    pprotein "github.com/kaz/pprotein/integration/echov4"
)

func main() {
    // pprof の handler を追加
    pprotein.Integrate(e)
}

func initializeHandler(c echo.Context) error {
    // 計測開始
    http.Get("http://localhost:9000/api/group/collect")
}
```

```bash
go mod tidy
```

pprotein http log の集約
```bash
# この結果を pprotein の設定にペースト
task -t SetupTasks.yml generate-matching-groups
```

### 計測

デプロイすれば準備完了
```bash
task deploy
```

## Taskfile の補足
基本的な説明は表示される help を見てください
```bash
task
```

分かりにくそう or 便利なものを補足します

`SetupTasks.yml`
```bash
# 競技サーバーに s1, s2, ... の hostname を付けることで,
# 設定ファイルを追加で読み込むことができるようになります
task -t SetupTasks.yml hostname -- s1

# 2台目以降のセットアップ用
task -t SetupTasks.yml sub
```

`Taskfile.yml`
```bash
# origin/my-branch が反映されます
# 何も指定しない場合は origin/main が反映されます
task deploy -- my-branch

# service の起動 & 自動起動を有効化
task enable -- nginx mysql

# service の停止 & 自動起動を無効化
task disable -- nginx mysql

# 変数に設定したすべての service を再起動
task restart-all

# 変数に設定したすべての service の状態を表示
task status-all
```

`Taskfile.yml` は `~` にシンボリックリンクを置いていて, `alias task="task -g"` を設定しているので, どこからでも利用できます.

## リンク集

ライブラリ

- https://github.com/jmoiron/sqlx/
- https://jmoiron.github.io/sqlx/
- https://github.com/samber/lo/
- https://echo.labstack.com/docs/

ミドルウェア

- https://nginx.org/en/docs/

その他

- https://go.dev/play/