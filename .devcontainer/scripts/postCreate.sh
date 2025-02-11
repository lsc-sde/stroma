git config --global --add safe.directory /workspaces/stroma
cd /workspaces/stroma
pre-commit install

cat >> ~/.inputrc <<'EOF'
"\e[A": history-search-backward
"\e[B": history-search-forward
EOF
