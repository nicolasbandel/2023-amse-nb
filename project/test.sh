echo "loading requiredments"
pip install -r ./main/project/requirements.txt 
echo "running test.py"
python ./main/project/tests.py
find ./ -type f 