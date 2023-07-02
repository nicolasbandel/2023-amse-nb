import os

automatedDataPipelinePath = "./project/data/AutomatedDataPipeline.py"
dataFilterPath = "./project/DataFilter.py"
dataProcessorPath = "./project/DataProcessor.py"

def runPythonSkript(path):
    command = "python " + os.path.abspath(path)
    os.system(command)

def main():
    print("AutomatedDataPipeline")
    runPythonSkript(automatedDataPipelinePath)
    print("DataFilter")
    runPythonSkript(dataFilterPath)
    print("DataProcessor")
    runPythonSkript(dataProcessorPath)

if __name__ == "__main__":
    main()