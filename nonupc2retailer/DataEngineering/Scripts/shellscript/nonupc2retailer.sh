
PROJECT_DIR="/home/ubuntu/Automation/nonupc2retailer/DataEngineering/Scripts/"
JAR_LOC="/home/ubuntu/Automation/nonupc2retailer/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"
PYTHON_FILE="NonUPCAutomation.py"
LOAD_TYPE="full"

cd $PROJECT_DIR
python $PYTHON_FILE $JAR_LOC $LOAD_TYPE
