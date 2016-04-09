export OS_USERNAME=admin
export OS_PASSWORD=password
export OS_TENANT_NAME=admin
export OS_AUTH_URL=http://10.5.8.65:5000/

nova live-migrate $1 $2
