import win32com.client as client
import pathlib  ### just manipulate path strings and output the workspace's master path ####

excel_path = pathlib.Path('daily_report.xlsx')
print(excel_path)
print(excel_path.absolute())
outlook = client.Dispatch("Outlook.Application")
message = outlook.CreateItem(0)


#Toggle Display
#message.Display()

####################### Load Email List ########################
message.To = "xcheng@tplhk.cntaiping.com"
message.CC = "xcheng@tplhk.cntaiping.com"
message.BCC = "xcheng@tplhk.cntaiping.com"

################## Set Subject Message

message.Subject = "Test 1"
message.Body = "Test Message Body"

############### Setting HTML FANCY FORMAT ##########################
message.HTMLBody = "<b> wishing you all the best on your birthday! </b>"
message.Attachments.Add(str(excel_path.absolute()))
message.Send()



#######