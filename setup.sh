echo $1
if [[ $1 == "block" ]]; then
    curl https://api.github.com/repos/Antares0982/PikaInterface/contents/pika_interface_blocking.py | jq -r ".content" | base64 --decode > antares_pika_interface.py
else
    curl https://api.github.com/repos/Antares0982/PikaInterface/contents/pika_interface.py | jq -r ".content" | base64 --decode > antares_pika_interface.py
fi
