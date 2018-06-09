PROJECT_DIR="/home/ubuntu/Automation/impactradius/DataEngineering/Scripts/"
JAR_LOC="/home/ubuntu/Automation/impactradius/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"
PYTHON_FILE="ImpalseRadiusAutomation.py"
LOAD_TYPE="full"

cd $PROJECT_DIR
python $PYTHON_FILE $JAR_LOC $LOAD_TYPE
