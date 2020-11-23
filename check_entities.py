import time
import csv
import datetime
from os import listdir
from nltk.tokenize import sent_tokenize
import re
import spacy
nlp = spacy.load('en_core_web_sm')
import json
import glob
import urllib
import urllib.parse
import urllib.request
from bs4 import BeautifulSoup
import string
import argparse

punc = set(string.punctuation) - {'.'}
punc = ''.join(punc)
num = '0123456789'
# queried_list = []

def extract_entities(sentences, filename, q_list):
	# global queried_list
	c =0
	name = str(filename.split('/')[-1].split('_')[3][:4]) + ' - ' + str(filename.split('/')[-1].split('_')[1][:4]) + ':'
	for i, sentence in enumerate(sentences):
		doc = nlp(sentence)
		entity = ''
		iobs = ''
		ent_types = ''
		for x in doc.ents:
			if str(x) not in queried_list:
				c += 1
				q_list.append(str(x))
	print(name,c)

def clean(bs):
	final = ''
	for tag in bs.find_all():
		if (tag not in ['filestats', 'filename']) and (len(tag.text) > 500):
			text = tag.text.replace('\n', ' ')
			text = text.replace('\t', ' ')
			text = text.translate(str.maketrans('', '', punc))
			text = text.translate(str.maketrans('', '', num))
			text = re.sub('\s{2,}', ' ',text)
			final += text
	return final

def run_nen(filenames, q_list):

	for i, filename in enumerate(filenames):
		if(i==0):
			print('For company : ',filename.split('/')[-1].split('_')[0])
		try:
			f = open(filename)
			soup = f.read()
			bs = BeautifulSoup(soup,"html.parser")
			clean_text = clean(bs)
			sentences = sent_tokenize(clean_text)
			extract_entities(sentences, filename, q_list)
		except Exception as e:
			print(e)

if __name__ == '__main__':

	path = '/home/saiamrit/IRE_major_project/10k_1500_10_org/'
	# companies = ['1520006', '1400810', '320335', '878927', '1021162']
	companies = ['1306830', '719739']
	listify = []
	for c in companies:
		queried_list = []
		ly = sorted(glob.glob(path + '{}*'.format(c)))
		listify.extend(ly)
		print(listify)
		print('-'*100)
		run_nen(listify, queried_list)
		listify = []
		q_list = []

	print(len(companies))
	print(len(listify))

	# run_nen()