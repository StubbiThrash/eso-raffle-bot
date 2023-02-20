import datetime
import linecache
import math
import time
import warnings
import numpy as np
import pandas as pd
import discord
import requests


warnings.simplefilter(action='ignore', category=FutureWarning)
intents = discord.Intents.all()
client = discord.Client(command_prefix='/', intents=intents)




@client.event
async def on_ready():
    print('We have logged in as {0.user}'.format(client))

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('Datum:'):

        with open('GBLData.lua') as fp:
            print(fp)
            r = requests.get(message.attachments[0].url, allow_redirects=True)
            print(r)
            file = open('GBLData.lua', 'wb').write(r.content)
            print(file)
            msg = message.content
            print(msg)
            startdate = msg.split(":")[1].split("-")[0]
            print(startdate)
            enddate = msg.split("-")[1].split(",")[0]
            print(enddate)
            lospreis = msg.split(",")[4]
            print(lospreis)
            jackpotp1 = float(msg.split(",")[1])
            jackpotp2 = float(msg.split(",")[2])
            jackpotp3 = float(msg.split(",")[3])
            print(jackpotp1)
            print(jackpotp2)
            print(jackpotp3)
            checkp = jackpotp1 + jackpotp2 + jackpotp3
            gildengoldp = 1.0 - jackpotp1 - jackpotp2 - jackpotp3
            gildengoldp = gildengoldp.__round__(2)
            lospreis = int(lospreis)
            file = open("GBLData.lua")
            data = []
            df = pd.DataFrame()
            debug = 1
            if debug == 1:
                print("prozent gesamt jackpots: ")
                print(checkp)
                print("gilden prozent: ")
                print(gildengoldp)
            # Ermittlung der Goldeinzahlungen aus der Datei
            for num, line in enumerate(file, 1):
                if "dep_gold" in line:
                    los_count = 0
                    currentline = linecache.getline("GBLData.lua", num)
                    # Bereinigung der Zeile
                    if debug == 1:
                        print(currentline)
                    currentline = currentline.replace("\n", "").replace(" ", "").replace(",", "").replace('"', '')
                    if debug == 1:
                        print(currentline)
                    currentline = currentline.split("=")[1]
                    if debug == 1:
                        print(currentline)
                    # Ermittlung der Daten aus der Zeile
                    timestamp = currentline.split("\\t")[0]  # timestamp
                    user = currentline.split("\\t")[1]  # user
                    trans_type = currentline.split("\\t")[2]  # trans_type
                    gold_ct = int(currentline.split("\\t")[3])  # gold
                    if debug == 1:
                        print("#######################################")
                        print(timestamp)
                        print(user)
                        print(trans_type)
                        print(gold_ct)
                    # Ermittlung Anzahl Lose Es wird geschaut ob der eingezahlte Betrag ohne Rest teilbar ist durch den Lospreis
                    raffle = int(gold_ct)
                    if debug == 1:
                        print("raffle:")
                        print(raffle)
                        print("POG")
                    if raffle % int(lospreis) == 0:
                        los_count = int(raffle / lospreis)
                        if debug == 1:
                            print("Anzahl Lose:")
                            print(los_count)
                    data = {"Einzahlung": gold_ct, "Zeitstempel": timestamp, "Transaktionr": trans_type, "User": user,
                            "Anzahl Lose": los_count, }
                    df = df.append(data, ignore_index=True)
                    if debug == 1:
                        print(data)
                        print('new deposite')
            if debug == 1:
                print("DATAFRAME################################:")
            df.to_csv('out.csv', header=None, sep=';')
            # Gewinnermittlung
            df_entries = pd.DataFrame()
            startdate_ts = time.mktime(datetime.datetime.strptime(startdate, "%d.%m.%Y").timetuple())
            if debug == 1:
                print(startdate_ts)
            enddate_ts = time.mktime(datetime.datetime.strptime(enddate, "%d.%m.%Y").timetuple())
            enddate_ts = enddate_ts + 86399
            if debug == 1:
                print(enddate_ts)
                print(datetime.datetime.fromtimestamp(enddate_ts))
            if debug == 1:
                print("ermittel Datensätze:")
            df_entries = df.loc[
                (df['Zeitstempel'].astype('int') >= int(startdate_ts)) & (
                        df['Zeitstempel'].astype('int') <= int(enddate_ts)) & (
                        df['Anzahl Lose'].astype('int') > 0)]
            if debug == 1:
                print(df_entries)
            entrie_value = df_entries['Einzahlung'].astype('int').sum()
            if debug == 1:
                print(entrie_value)
            jackpot1 = math.trunc(entrie_value * jackpotp1)
            jackpot2 = math.trunc(entrie_value * jackpotp2)
            jackpot3 = math.trunc(entrie_value * jackpotp3)
            guildgold = math.trunc(entrie_value * gildengoldp)
            if debug == 1:
                print(jackpot1)
                print(jackpot2)
                print(jackpot3)
                print(guildgold)
            df_raffle_col = ["Einzahlung", "Zeitstempel", "Transaktionr", "User", "Anzahl Lose"]
            df_raffle = pd.DataFrame(np.repeat(df_entries.values, df_entries['Anzahl Lose'].astype('int'), axis=0))
            df_raffle = pd.DataFrame(data=df_raffle.values, columns=df_raffle_col)
            if debug == 1:
                print(df_raffle)
            df_raffle.to_csv('outwinners.csv', header=None, sep=';')
            winner1f = 0
            winner2f = 0
            winner3f = 0
            # noinspection PyBroadException
            try:
                # winner 1
                winner1 = df_raffle.sample()
                if debug == 1:
                    print("winner1:")
                    print(winner1)
                winner1_str = winner1["User"].values[0]
                if debug == 1:
                    print("winner1_str;: #######")
                    print(winner1_str)
                df_raffle = df_raffle.loc[df_raffle["User"].astype("string") != winner1["User"].values[0]]
                if debug == 1:
                    print(df_raffle)
            except:
                print("kein winner 1")
                winner1f = 1
            try:
                # winner 2
                winner2 = df_raffle.sample()
                if debug == 1:
                    print("winner2:")
                    print(winner2)
                winner2_str = winner2["User"].values[0]
                if debug == 1:
                    print("winner2_str;: #######")
                    print(winner2_str)
                df_raffle = df_raffle.loc[df_raffle["User"].astype("string") != winner2["User"].values[0]]
                if debug == 1:
                    print(df_raffle)
            except:
                print("kein winner 2")
                winner2f = 1
            try:
                # winner 3
                winner3 = df_raffle.sample()
                if debug == 1:
                    print("winner3:")
                    print(winner3)
                winner3_str = winner3["User"].values[0]
                if debug == 1:
                    print("winner3_str;: #######")
                    print(winner3_str)
                df_raffle = df_raffle.loc[df_raffle["User"].astype("string") != winner3["User"].values[0]]
                if debug == 1:
                    print(df_raffle)
            except:
                print("kein winner 3")
                winner3f = 1
            # wenn es weniger als 3 gewinner gibt werden die jackpots entsprechend auf den Gildengold-Anteil gerechnet
            if winner1f == 1:
                guildgold = guildgold + jackpot1
            if winner2f == 1:
                guildgold = guildgold + jackpot2
            if winner3f == 1:
                guildgold = guildgold + jackpot3
            # Ausgabe der Gewinner
            botmsg = "###############################\n"
            botmsg = botmsg + "Gewinner vom " + enddate + ":\n"
            if winner1f == 0:
                print("Platz 1: " + winner1["User"].values[0] + " gewinnt " + str(jackpot1) + " Gold\n")
                botmsg = botmsg + "Platz 1: " + winner1["User"].values[0] + " gewinnt " + str(jackpot1) + " Gold\n"
            if winner2f == 0:
                print("Platz 2: " + winner2["User"].values[0] + " gewinnt " + str(jackpot2) + " Gold\n")
                botmsg = botmsg + "Platz 2: " + winner2["User"].values[0] + " gewinnt " + str(jackpot2) + " Gold\n"
            if winner3f == 0:
                print("Platz 3: " + winner3["User"].values[0] + " gewinnt " + str(jackpot3) + " Gold\n")
                botmsg = botmsg + "Platz 3: " + winner3["User"].values[0] + " gewinnt " + str(jackpot3) + " Gold\n"
            botmsg = botmsg + "\n\nGildenbankeinnahmen durch Lotto: " + str(guildgold) + " Gold"
            botmsg = botmsg + "\n\nHerzlichen Glückwunsch an die Gewinner!\n"
            botmsg = botmsg + "###############################"
            if debug == 1:
                print(datetime.datetime.fromtimestamp(enddate_ts))
            # 1675292399 ------ 86399 ticks
            # 1675206000
            file.close()
            data.clear()

        await message.channel.send(botmsg)
async def on_message(self, messsage):
    prefix="/test"
    if messsage.content.startswith(prefix):
        await messsage.channel.send("Hello!")

client.run('Add Your API Token here!')
