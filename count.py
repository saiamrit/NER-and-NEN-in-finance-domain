import argparse
from os import listdir
from os.path import isfile, join
import json
import math 
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True,
                        help="CONLL files path")
    parser.add_argument("--output_path", required=True,
                        help="Path to save output JSON files")

    args = parser.parse_args()

    filenames = [f for f in listdir(args.input_path) if isfile(join(args.input_path, f)) and  f.endswith(".conll")]
    # train_file = open(args.output_path + '/train.json', 'a')
    # val_file = open(args.output_path + '/dev.json', 'a')
    # test_file = open(args.output_path + '/test.json', 'a')
    # final = []
    sent = 0
    for filename in filenames:
        with open(args.input_path + '/' + filename, 'r') as f:
            data = f.readlines()
        # sentences = []
        # sentence = []
        # labels = []
        # label = []
        for line in data:
            if line != '\n':
                pass
                # words = line.split()
                # sentence.append(words[0])
                # label.append(words[-1])
            else: 
                sent += 1  
                # sentences.append(sentence)
                # labels.append(label)
                # sentence = []
                # label = []

    print("Files = ",len(filenames))
    print("Total sentences = ",sent)

if __name__ == "__main__":
    main()