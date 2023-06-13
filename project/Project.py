import os

automatedDataPipelinePath = "./AutomatedDataPipeline.py"
dataFilterPath = "./DataFilter.py"
dataProcessorPath = "./DataProcessor.py"

def runPythonSkript(path):
    command = "python " + os.path.abspath(path)
    os.system(command)

def main():
    runPythonSkript(automatedDataPipelinePath)
    runPythonSkript(dataFilterPath)
    runPythonSkript(dataProcessorPath)

if __name__ == "__main__":
    main()