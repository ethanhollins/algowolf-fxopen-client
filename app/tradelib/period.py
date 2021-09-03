'''
Periods
'''
TICK = 'TICK'
FIVE_SECONDS = 'S5'
ONE_MINUTE = 'M1'
TWO_MINUTES = 'M2'
THREE_MINUTES = 'M3'
FOUR_MINUTES = 'M4'
FIVE_MINUTES = 'M5'
TEN_MINUTES = 'M10'
FIFTEEN_MINUTES= 'M15'
THIRTY_MINUTES = 'M30'
ONE_HOUR = 'H1'
TWO_HOURS = 'H2'
THREE_HOURS = 'H3'
FOUR_HOURS = 'H4'
TWELVE_HOURS = 'H12'
DAILY = 'D'
WEEKLY = 'W'
MONTHLY = 'M'

def getPeriodOffsetSeconds(period):
	if period == FIVE_SECONDS:
		return 5
	elif period == ONE_MINUTE:
		return 60*1
	elif period == TWO_MINUTES:
		return 60*2
	elif period == THREE_MINUTES:
		return 60*3
	elif period == FIVE_MINUTES:
		return 60*5
	elif period == TEN_MINUTES:
		return 60*10
	elif period == FIFTEEN_MINUTES:
		return 60*15
	elif period == THIRTY_MINUTES:
		return 60*30
	elif period == ONE_HOUR:
		return 60*60
	elif period == TWO_HOURS:
		return 60*60*2
	elif period == THREE_HOURS:
		return 60*60*3
	elif period == FOUR_HOURS:
		return 60*60*4
	elif period == DAILY:
		return 60*60*24
	elif period == WEEKLY:
		return 60*60*24*7
	elif period == MONTHLY:
		return 60*60*24*7*4
	else:
		None


