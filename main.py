import requests
import json
from time import sleep
import numpy as np
import pandas as pd
import re


# Default wall_id is LK group wall
def get_participants(access_token, post_id, wall_id=-128201573):
	get_comments_url = "https://api.vk.com/method/wall.getComments"
	participants_id = []

	respond = requests.get(get_comments_url, params={"v": "5.126", "access_token": access_token,
	                                "owner_id": wall_id, "post_id": post_id, "count": "1"})
	js = json.loads(respond.text)
	if "error" in js:
		print("Error code: {}\nError text: {}".format(js["error"]["error_code"], js["error"]["error_msg"]))
	comments_cnt = js["response"]["count"]
	message_cnt = 0
	processed_cnt = 0

	print("Comment count: {}".format(comments_cnt))

	while message_cnt < comments_cnt:
		respond = requests.get(get_comments_url, params={"v": "5.126", "access_token": access_token,
		                                                 "owner_id": wall_id, "post_id": post_id,
		                                                 "count": 50, "offset": processed_cnt})
		js = json.loads(respond.text)
		# print(js)

		for comment in js["response"]["items"]:
			processed_cnt += 1
			message_cnt += 1
			if comment["thread"]["count"] > 0:
				message_cnt += comment["thread"]["count"]
				comment_id = comment["id"]
				print("Response to comment {}: {}".format(comment["id"], comment["text"]))
				respond = requests.get(get_comments_url, params={"v": "5.126", "access_token": access_token,
				                                                 "comment_id": comment_id, "owner_id": wall_id})
				js = json.loads(respond.text)
				for item in js["response"]["items"]:
					print("\t id{}: {}".format(item["from_id"], item["text"]))

			if comment["text"] == "+":
				participants_id.append(comment["from_id"])
			else:
				print("Unexpected comment from id{}: \"{}\"".format(comment["from_id"], comment["text"]))
		sleep(0.5)

	ids = ",".join([str(user_id) for user_id in participants_id])
	get_name_url = "https://api.vk.com/method/users.get"
	get = requests.get(get_name_url, params={"v": "5.126", "access_token": access_token,
		                                     "user_ids": ids, "fields": "first_name,last_name"})
	js = json.loads(get.text)
	participants = {item["id"]: "{} {}".format(item["first_name"], item["last_name"]) for item in js["response"]}

	participants = pd.DataFrame([tuple(["vk.com/id{}".format(key), value]) for key, value in participants.items()],
	                             columns=["link", "name"])
	return participants


def make_mailing(access_token, data):
	vk_pattern = re.compile("vk\.com/id\d+")

	for key, row in data.iterrows():
		if re.match(vk_pattern, row["killer_link"]):
			recipient = row["killer_link"].replace("vk.com/id", "")
			text = get_text(row)
			send_messages(access_token, recipient, text)
			sleep(0.5)
		else:
			print("ERROR: Invalid vk link for recipient: {}. Message not send.".format(row["killer_link"]))


def send_messages(access_token, recipient, text):
	base_url = "https://api.vk.com/method/messages.send"
	get = requests.get(base_url, params={"message": text, "random_id": "0", "peer_id": recipient, "v": "5.126",
	                                     "access_token": access_token})
	js = json.loads(get.text)
	if "error" in js:
		print("user [{}] ERROR:".format(recipient))
		print("\tError code: {}\n\tError text: {}".format(js["error"]["error_code"], js["error"]["error_msg"]))
		# print("\tRecipient: {}\n\tMessage: \"{}\"".format(recipient, text))
		return False
	print("user [{}] OK".format(recipient))
	return True


def get_text(row):
	vk_pattern = re.compile("vk\.com/id\d+")
	tg_pattern = re.compile("t\.me/.*")
	string = "#ИГРА_КИЛЛЕР\n"
	if re.match(vk_pattern, row["victim_link"]):
		user_id = row["victim_link"].replace("vk.com/id", "")
		string += "Привет, твоя жертва @id{} ({}) 😈".format(user_id, row["victim_name"])
	elif re.match(tg_pattern, row["victim_link"]):
		string += "Привет, твоя жертва {}. ".format(row["victim_name"])
		string += "У неё нет страницы в вк, поэтому вот ссылка на telegram: {} 😈".format(row["victim_link"])
	string += "\n\nГде она живёт? Я не скажу, это тебе надо выяснить самостоятельно. "
	string += "По остальным возникшим вопросам обращайся к @id6437630 (Сергею Кононову)."

	return string


if __name__=="__main__":
	url = "https://oauth.vk.com/authorize?client_id=7740530&display=page&redirect_uri=https://oauth.vk.com/blank.html&scope=0&response_type=token&v=5.126"
	access_token = "60d15f8cc6e03bbab63792a02aadd02c773d522a69dd96ced79df1f59a51ed1219415e0590658995833be"
	community_token = "6277a1fc69d5f089f9bf7f4ec8a32d60d1eb6fe9c8cb24acec29810af8f12a3e3af39ad9aa3bf8c57042e"

	# Load post comments and filer participants which post "+"
	# Return participants vk link and first and last name
	def load():
		participants = get_participants(access_token, wall_id=-128201573, post_id=9035)
		# participants = get_participants(access_token, wall_id=-128201573, post_id=5318)
		participants.to_csv("particioants.tsv", sep="\t", index=False)

	# Load participants and shuffle them
	def process():
		participants = pd.read_csv("particioants.tsv", sep="\t")
		participants = participants.sample(frac=1).reset_index(drop=True)

		# Form data in suitable format
		participants.rename(columns={"link": "killer_link", "name": "killer_name"}, inplace=True)
		participants["victim_link"] = participants["killer_link"][1:].to_list() + [participants["killer_link"][0], ]
		participants["victim_name"] = participants["killer_name"][1:].to_list() + [participants["killer_name"][0], ]
		participants.to_csv("data.tsv", sep="\t", index=False)

	# Load data and send message to participants
	def send():
		data = pd.read_csv("data.head.tsv", sep="\t")
		make_mailing(community_token, data)

	# load()
	# process()
	send()