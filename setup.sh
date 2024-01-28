echo $1
if [[ $1 == "block" ]]; then
    curl https://api.github.com/repos/Antares0982/RabbitMQInterface/contents/rabbitmq_interface_blocking.py | jq -r ".content" | base64 --decode > rabbitmq_interface.py
else
    curl https://api.github.com/repos/Antares0982/RabbitMQInterface/contents/rabbitmq_interface.py | jq -r ".content" | base64 --decode > rabbitmq_interface.py
fi
