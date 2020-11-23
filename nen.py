import multiprocessing
from multiprocessing import Pool
import time
import csv
import datetime
import numpy as np
from os import listdir
from nltk.tokenize import sent_tokenize
import re
import spacy
nlp = spacy.load('en_core_web_sm')
import json
import pickle
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
queried_dict = {}
label_dict = {}
# num_entities = []

def open_file(filename):
	file = open(filename,encoding="utf-8")
	return file

def chunks(self, l, n):
	for i in range(0, len(l), n):
		yield l[i:i + n]

def remove_html_tags(text):
	"""Remove html tags from a string"""
	clean = re.compile('<.*?>')
	return re.sub(clean, ' ', text)

def replace(string, char): 
	pattern = char + '{2,}'
	string = re.sub(pattern, char, string) 
	return string 

def get_companiesList():
	with open('df_10k_1500_10_org.pkl', 'rb') as f:
		data = pickle.load(f)
		companies = list(np.unique(data['comp_name']))
		return companies 


def extract_labels(query):
	# global label_dict
	api_key = "AIzaSyCTQWJQTUyHCfarsBFGkVIgmrbnI91H1lg"
	service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
	headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
	label = 0


	if query not in queried_dict.keys():
		desc = ''
		params = {
			'query': query,
			'limit': 1,
			'indent': True,
			'key': api_key,
		}
		url = service_url + '?' + urllib.parse.urlencode(params)
		req = urllib.request.Request(url, headers=headers)
		response = json.loads(urllib.request.urlopen(req).read())
		for i,element in enumerate(response['itemListElement']):

			if('description' in element['result'].keys()):
				desc = str(element['result']['description'])

			if('Organization' in element['result']['@type']):
				label = 'org'
			# elif('Place' in element['result']['@type']):
			# 	label = 'Place'
			# elif('Person' in element['result']['@type']):
			# 	label = 'Person'
			# elif('Thing' in element['result']['@type']):
			# 	label = 'Thing'

		if(desc != ''):
			# print(desc)
			if('company' in desc.lower() or label == 'org'):  
				label='ORG'
				companies = get_companiesList()
				name = str(element['result']['name']).lower()   

				for co in companies:
					if((name in co) or (co in name)):
						# print(co)
						if(co in label_dict.keys()):
							label_dict[co].append(query)
						else:
							label_dict[co] = []
							label_dict[co].append(query)

		# elif(label=='Person'):
		# 	label='PER'
		# elif(label=='Place'):
		# 	label='LOC'
		# elif(label == 'Thing'):
		# 	label = 'PDT'
		# else:
		# 	label = 0
		queried_dict[query] = label
		# print(label_dict)
	else:
		return queried_dict[query]

	return label

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


def extract_entities(sentences, filename):
	# name = filename.split('/')[-1].split('.')[0]
	# out_path = '/home/saiamrit/IRE_major_project/'
	# f = open(out_path + name + '.conll' , 'w')

	for i, sentence in enumerate(sentences):
		doc = nlp(sentence)
		entity = ''
		iobs = ''
		ent_types = ''
		for x in doc:

			if(x.ent_iob_ == 'O'):
				if entity == '':
					continue
					# f.write(str(x) + ' ' + '0 0\n')
				else:
					label = extract_labels(entity)
					# for item, iob, ent_type in zip(entity.split(), iobs.split(), ent_types.split()): 
					# 	pass
						# f.write(str(item) + ' ' + str(iob) + '-' + str(ent_type) + ' ' + str(label) + '\n')
					entity = ''
					iobs = ''
					ent_types = ''
			elif x.ent_type_ in ['PERSON','ORG','GPE','LOC','PRODUCT']:
				entity += ' ' + str(x)
				iobs += ' ' + str(x.ent_iob_)
				ent_types += ' ' + str(x.ent_type_)
			else:
				pass
				# f.write(str(x) + ' ' + '0 0\n')
		# f.write('\n')

def parse_chunk(filenames):
	results = []
	# label_dict = {}
	for i, filename in enumerate(filenames):
		# try:
		print(filename)
		f = open(filename)
		soup = f.read()
		bs = BeautifulSoup(soup,"html.parser")
		clean_text = clean(bs)
		sentences = sent_tokenize(clean_text)
		entities = extract_entities(sentences, filename)
			# print(label_dict)
		# except Exception as e:
		# 	print(e)
	# print(num_entities)
	return results

def run_nen():
	# global label_dict
	path = '/home/saiamrit/IRE_major_project/10k_1500_10_org/'
	# companies = ['1520006', '1400810', '320335', '878927', '1021162']
	# companies = [320193, 72971]
	companies = ['37996', '1555280', '1015780', '899629', '819220', '40211', '813672', '1002047', '320193', '72971',
		 '729986', '315709', '1525221', '712515', '1107843', '100493', '1552800', '1113232', '1113481', '1580670', 
		 '2488', '56978', '1090727', '1357204', '1103982', '896159', '1500217', '103730', '1616318', '1031296', 
		 '745732', '317540', '1559865', '76605', '833079', '1126956', '898174', '1283699', '866729', '729580', 
		 '883984', '1530721', '1407623', '64040', '1356576', '1136893', '1585689', '1300514', '1144980', '87347', 
		 '36104', '794172', '1601712', '795403', '896878', '1596783', '1604778', '313143', '949039', '910073', 
		 '897077', '1707925', '1551182', '876427', '202058', '78003', '707549', '1053507', '1597672', '73124', 
		 '795266', '1710366', '731802', '110621', '1071739', '1534992', '1042046', '1478242', '1669812', '1035002', 
		 '1169561', '898293', '72207', '12208', '66382', '21665', '705432', '832101', '10456', '907254', '1381197', 
		 '70145', '6769', '1278021', '780571', '1581091', '1201792', '921738', '22444', '63908', '716643', '926326']
	listify = []
	# label_dict = {}
	yrs = ['2007' , '2012' , '2018']
	for c in companies:
		for y in yrs:
			ly = sorted(glob.glob(path + '{}_{}*_10-K_*'.format(c,y)))
			listify.extend(ly)

	print(len(companies))
	print(len(listify))

	for i, filename in enumerate(listify):
		try:
			print(filename)
			f = open(filename)
			soup = f.read()
			bs = BeautifulSoup(soup,"html.parser")
			clean_text = clean(bs)
			sentences = sent_tokenize(clean_text)
			entities = extract_entities(sentences, filename)
			# print(label_dict)
		except Exception as e:
			print(e)

if __name__ == '__main__':
	# label_dict = {}
	# global label_dict
	# parser = argparse.ArgumentParser()
	# parser.add_argument("--input_path", required=True,
	# 					help="Documents path")
	# parser.add_argument("--output_path", required=True,
	# 					help="Path to save output CONLL files")

	# args = parser.parse_args()

	run_nen()
	# print(label_dict)

	with open("nen.txt", "w") as outfile:  
		json.dump(label_dict, outfile)