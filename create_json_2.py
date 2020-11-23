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

    # /home/saiamrit/IRE_major_project/conll_latest/jsons/

    args = parser.parse_args()

    filenames = [f for f in listdir(args.input_path) if isfile(join(args.input_path, f)) and  f.endswith(".conll")]
    train_file = open(args.output_path + '/train.json', 'a')
    val_file = open(args.output_path + '/dev.json', 'a')
    test_file = open(args.output_path + '/test.json', 'a')
    final = []
    for filename in filenames:
        with open(args.input_path + '/' + filename, 'r') as f:
            data = f.readlines()
        sentences = []
        sentence = []
        labels = []
        label = []
        spacy = []
        spacy_labels = []
        for line in data:
            if line != '\n':
                words = line.split()
                sentence.append(words[0])
                spacy.append(words[1])
                label.append(words[-1])
            else:   
                sentences.append(sentence)
                labels.append(label)
                spacy_labels.append(spacy)
                sentence = []
                label = []
                spacy = []

        label_dict = {'0': 0, 'PER': 1, 'Person': 1, 'ORG': 2, 'Thing': 3, 'PDT': 3, 'LOC': 4, 'Place': 4}
        
        prev = ''
        flag = 0
        for sent, label, spl in zip(sentences, labels, spacy_labels):
            # print(label)
            label = [label_dict[l] for l in label]
            # label_list = []
            # for l in label: 
            #     if l != '0' and l == prev:
            #         flag += 4
            #         label_list.append(label_dict[l] + flag)
            #     elif l != '0' and l != prev:
            #         flag = 0
            #         label_list.append(label_dict[l])
            #     else: 
            #         flag = 0
            #         label_list.append(label_dict[l])
            #     prev = l     
                    
            final.append({"str_words": sent, "tags": label, "spacy_labels": spl})
    final = [f for f in final if f != []]
    print('Total length', len(final))
    if final:
        train_split = math.floor(len(final) * 0.9)
        val_test_split = (len(final) - train_split) * 0.5
        train, val, test = final[:train_split], final[train_split + 1 : train_split + math.ceil(val_test_split) + 1], final[train_split + math.ceil(val_test_split) + 1: ]  
    print('Train length', len(train))
    print('Validation length', len(val))
    print('Test length', len(test))
    json.dump(train, train_file)
    json.dump(val, val_file)
    json.dump(test, test_file)

if __name__ == "__main__":
    main()