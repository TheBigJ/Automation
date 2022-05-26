import yagmail

def sendMailNotification(tenants, receiverMail):
    yag = yagmail.SMTP('deployment.monitor@gmail.com', 'takgczdcdxmzfvgd')
    contents = ['All the steps required for the migration completed successfully with out any '
                'error for the following tenants ' + tenants]
    yag.send(receiverMail, 'Migration Successfully completed.', contents)

#sendMailNotification('devfox, aosonair', 'abhijeets@sintecmedia.com')