echo "loading requiredments"
pip ./main/project/requirements.txt 
echo "running test.py"
python ./main/project/tests.py
find ./ -type f 