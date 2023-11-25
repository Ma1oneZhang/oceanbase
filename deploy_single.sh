set -e
pwd=$(pwd)
echo "current path: $pwd"
# echo "mv the binary observer to the data path: /your-data-path "
mkdir -p /home/$USER/observer/bin
# cp $pwd/build_release/src/observer/observer /home/$USER/observer/bin/
# gdb-add-index /home/$USER/observer/bin/observer
echo "start single"
python $pwd/deploy.py --cluster-home-path /home/$USER/observer 
# --self-host=true
#  --profile=true