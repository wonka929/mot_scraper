from telegram.bot import Bot
from telegram.ext import *
from telegram.update import Update, TelegramObject
from telegram import *
import mysql.connector
import datetime
from dateutil.relativedelta import *
from telegram.error import (TelegramError, Unauthorized, BadRequest, TimedOut, ChatMigrated, NetworkError)
import logging
from time import sleep
logger = logging.getLogger(__name__)


token = '5227412874:AAEItT9IS4y55YcXfkR_Ro6b74m6G4I8YK8'
db_user = 'tgbot'
db_pass = 'tgbot@123'
db = 'tgbot'
host = 'localhost'


def respons(update: Update, context: CallbackContext):
    try:
        chat_id = update.effective_chat.id
        f_name = update.effective_user.first_name
        text = update.message.text
        user_id = update.effective_user.id
        msg_id = update.message.message_id
        if text in ('/Start', '/start'):
            main_manu(chat_id, msg_id, f_name)
    except Exception as e:
        logger.error(e)

def c_back_respons(update: Update, context: CallbackContext):
    try:
        user_id = update.effective_user.id
        print(user_id)
        chat_id = update.effective_chat.id
        f_name = update.effective_user.first_name
        cb_data = update.callback_query.data
        print(cb_data)
        msg_id = update.callback_query.message.message_id
        if cb_data == 'sub':
            send_invoice(chat_id, msg_id)
        elif cb_data == 'membs':
            status(chat_id, msg_id, f_name)
        elif cb_data == 'back':
            main_manu(chat_id, msg_id, f_name)
        elif cb_data == 'pref':
            preferences(chat_id, msg_id, f_name)
        elif cb_data == 'select_man':
            select_manufacturer(chat_id, msg_id, f_name)
        elif cb_data == 'select_mod':
            select_model(user_id, chat_id, msg_id, f_name)
        elif cb_data.startswith('manuf@'):
            update_sql("""UPDATE PREFERENCES SET MANUFACTURER = '%s' WHERE U_ID = '%s';""" %(cb_data.split('@')[1],user_id))
        elif cb_data.startswith('model@'):
            update_sql("""UPDATE PREFERENCES SET MODEL = '%s' WHERE U_ID = '%s';""" %(cb_data.split('@')[1],user_id))
        elif cb_data == 'pref_list':
            pref_list(chat_id, msg_id, f_name)
            
    except Exception as e:
        logger.error(e)

def main_manu(chat_id, msg_id, f_name):
    try:    
        inline = '{"inline_keyboard":[[{"text":"Subscribe","callback_data":"sub"}],[{"text":"Membership status","callback_data":"membs"}, ' \
                 '{"text":"Selected Preference","callback_data":"pref"}], [{"text":"Support","url":"https://t.me/thechartgod"}]]}'
        bot.send_message(
            chat_id=chat_id,
            text=f"""Main menu""",
            reply_markup=inline
        )
    except Exception as e:
        logger.error(e)

def send_invoice(chat_id, msg_id):
    try:
        prices = [LabeledPrice(label='1 Month Subscription', amount=1000)]
        bot.send_invoice(
            chat_id=chat_id,
            title='Subscription',
            description='1 Month sub $10',
            payload='payment',
            provider_token='284685063:TEST:MTA2YmY0YTQyOGJh',
            currency='EUR',
            prices=prices
        )
    except Exception as e:
        logger.error(e)

def status(chat_id, msg_id, f_name):
    try:
        conn = mysql.connector.connect(user=db_user, password=db_pass, host=host, database=db)
        cur = conn.cursor()
        sql = """SELECT STATUS, EXP_DATE FROM USERS WHERE U_ID = '%s'""" %(chat_id)
        cur.execute(sql)
        info = cur.fetchone()
        EXP_date = info[1]
        status = info[0]
        inline = '{"inline_keyboard":[[{"text":"Back to Menu","callback_data":"back"}]]}'
        bot.send_message(
            chat_id=chat_id,
            text=f"""Name: {f_name}\nStatus: {status}\nExpire Date: {EXP_date}""",
            reply_markup=inline
        )
    except Exception as e:
        logger.error(e)

def checkout(update: Update, context: CallbackContext):
    try:
        print(update)
        f_name = update.effective_user.first_name
        u_name = update.effective_user.username
        u_id = update.effective_user.id
        use_date = datetime.datetime.now() + datetime.timedelta(month=1)
        conn = mysql.connector.connect(user=db_user, password=db_pass, host=host, database=db)
        cur = conn.cursor()
        sql = 'INSERT INTO USERS (U_ID, U_NAME , STATUS, EXP_DATE) VALUES (%s, %s, %s, %s)'
        value = (u_id, f_name, 'allow', use_date)
        try:
            cur.execute(sql, value)
            conn.commit()
        except mysql.connector.IntegrityError:
            sql = 'UPDATE USERS SET STATUS = %s, EXP_DATE = %s  WHERE U_ID = %s' %('allow', use_date, u_id)
            cur.execute(sql)
            conn.commit()
        conn.close()
        select_model(update)
    except Exception as e:
        logger.error(e)


def successful_payment_callback(update: Update, context: CallbackContext):
    print(update)

def update_sql(sql):
    try:
        print(db_user)
        conn = mysql.connector.connect(user=db_user, password=db_pass, host=host, database=db)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(e)
    
    
def select_model(user_id, chat_id, msg_id, f_name):
    try:
        conn = mysql.connector.connect(user=db_user, password=db_pass, host=host, database=db)
        cur = conn.cursor()
        
        sql = """SELECT MANUFACTURER FROM PREFERENCES WHERE U_ID = '%s'""" %(user_id)        
        cur.execute(sql)
        Manufacturer = cur.fetchall()[0]
        
        sql_selection = """SELECT DISTINCT Model FROM MODEL WHERE Manufacturer = '%s' order by Model""" %(Manufacturer)
        cur.execute(sql_selection)
        
        models = cur.fetchall()

        for split in range(int(len(models)/25)+1):
            but = ''
            slice = models[split*25:(split+1)*25]
            for r in range(len(slice)):
                but = but + '[{"text":"'+slice[r][0]+'","callback_data":"model@'+slice[r][0]+'"}],'
                inline = '{"inline_keyboard":['+but[:-1]+']}'
            print(split)
            if split==0:
                msg = f"Seleziona il modello:"
            else:
                msg = '...continua...'
            bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode='HTML',
                reply_markup=inline
            )
            sleep(1)
        inline= '{"inline_keyboard":[[{"text":"Back to Menu","callback_data":"pref_list"}]]}'
        bot.send_message(
                chat_id=chat_id,
                text='...terminato.',
                parse_mode='HTML',
                reply_markup=inline
                )

    except Exception as e:
        logger.error(e)

def select_manufacturer(chat_id, msg_id, f_name):
    
    
    try:
        conn = mysql.connector.connect(user=db_user, password=db_pass, host=host, database=db)
        cur = conn.cursor()
        sql = 'SELECT DISTINCT Manufacturer FROM MODEL order by Manufacturer'
        cur.execute(sql),
        companies = cur.fetchall()
        for split in range(int(len(companies)/25)+1):
            but = ''
            slice = companies[split*25:(split+1)*25]
            for r in range(len(slice)):
                but = but + '[{"text":"'+slice[r][0]+'","callback_data":"manuf@'+slice[r][0]+'"}],'
            inline = '{"inline_keyboard":['+but[:-1]+']}'

            if split==0:
                msg = f"Seleziona il produttore che stai cercando"
            elif split==int(len(companies)/25):
                msg = '...terminato.'
            else:
                msg = '...continua...'

            bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode='HTML',
                reply_markup=inline
                )
            sleep(1)
            
        inline= '{"inline_keyboard":[[{"text":"Back to Menu","callback_data":"pref_list"}]]}'
        bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode='HTML',
                reply_markup=inline
                )
        
    except Exception as e:
        logger.error(e)

def preferences(chat_id, msg_id, f_name):
    try:
        conn = mysql.connector.connect(user=db_user, password=db_pass, host=host, database=db)
        cur = conn.cursor()
        sql = """SELECT MANUFACTURER, MODEL, MIN_PRICE, MAX_PRICE, LOCATION, MAX_KM FROM PREFERENCES WHERE U_ID = '%s'""" %(chat_id)
        cur.execute(sql)
        info = cur.fetchone()
        MANUFACTURER = info[0]
        MODEL = info[1]
        MIN_PRICE = info[2]
        MAX_PRICE = info[3]
        LOCATION = info[4]
        MAX_KM = info[5]
        inline = '{"inline_keyboard":[[{"text":"Back to Menu","callback_data":"back"}, {"text":"Modifica preferenze", "callback_data" : "pref_list"}]]}'
        bot.send_message(
            chat_id=chat_id,
            text=f"""Manufacturer: {MANUFACTURER}\nModel: {MODEL}\nMiminum Prie: {MIN_PRICE}\nMaximum Price :{MAX_PRICE}\nLocation :{LOCATION}\nMax KM :{MAX_KM}""",
            reply_markup=inline
        )
    except Exception as e:
        logger.error(e)
    

    
def pref_list(chat_id, msg_id, f_name):
    inline = '{"inline_keyboard":[[{"text":"Back to Menu","callback_data":"back"}], [{"text":"Modifica Marca", "callback_data" : "select_man"}], [{"text":"Modifica Modello", "callback_data" : "select_mod"}]]}'
    bot.send_message(
        chat_id=chat_id,
        text=f"""Queste sono le opzioni da poter modificare:""",
        reply_markup=inline
    )
    

if __name__ == '__main__':
    bot = Bot(token)
    updater = Updater(token, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(MessageHandler(Filters.text, respons))
    dp.add_handler(CallbackQueryHandler(c_back_respons))
    dp.add_handler(PreCheckoutQueryHandler(checkout))
    dp.add_handler(MessageHandler(Filters.successful_payment, successful_payment_callback))
    print("Rimango in attesa...")

    updater.start_polling()
    updater.idle()
