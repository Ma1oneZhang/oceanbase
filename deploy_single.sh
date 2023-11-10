set -e
pwd=$(pwd)
echo "current path: $pwd"
echo "mv the binary observer to the data path: /your-data-path "

# cp $pwd/build_release/src/observer/observer ~/observer/bin/
echo "start single"
python $pwd/deploy.py --cluster-home-path /your-data-path --self-host=true
#  --profile=false
