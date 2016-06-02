/************************************************************************************************************************************
  Class             :  EDI810OB
  Author            :  HiBiZ Solutions
  Date Created      :  02/16/2016
  Purpose           :  Class used to validate customer invoice and customer invoice line. It extract edi invoice and edi invoice line
  Input Object      :  Events, Customer invoice and customer invoice lines
  Output Object     :  EDI invoice and EDI invoice lines
  Dependent Classes :  KNDY4.ExceptionServiceAPI , EDIUtility_Static , EDI.EDIService , EDI.EDIService.Response and EdiExceptionUtility
************************************************************************************************************************************/
global class EDI810OB implements Database.Batchable<sObject>,Database.Stateful,KNDY4.Process.Runnable {
    global List<KNDY4.Notify.Message> errMessages = new List<KNDY4.Notify.Message>();
    global static List<KNDY4.Notify.Message> messages = new List<KNDY4.Notify.Message>();
    global static List<EDI__Event__c> validEdieventList = new List<EDI__Event__c>();
    /**
   * @author HiBiZ Solutions
   * @date 02/16/2016
   * @description Class that extends exception.
   *
   */
    public class EDI810OBException extends Exception { }
    /**
     * @description The batch class is invoked by calling Database.executeBatch and passing it the instance of the batch class .It Accepts company Id as parameter and executes the batch.
     *
     * @param company Id.
     * @return void.
     */
    global void runBatch(Id companyId) {
       EDI__Process__c proc;
        try {
            proc = [SELECT Id,EDI__Batch_Size__c From EDI__Process__c WHERE Name = 'SNDINV' LIMIT 1];
        } catch (Exception e) {
            throw new EDI810OBException('Process SNDINV has not been set up');
        }
        if (proc != null && proc.EDI__Batch_Size__c != null)
            Database.executeBatch(this,Integer.valueOf(proc.EDI__Batch_Size__c));
        else
            Database.executeBatch(this);
    }
   /**
    * @description This method is called at the beginning of a batch Apex job. It collects the records or objects to be passed to the interface method execute.It Accepts BatchableContext object as parameter and returns the list of EDI events
    *
    * @param Database.BatchableContext
    * @return List of Edi events
    */
    global List<EDI__Event__c> start(Database.BatchableContext bc) {
        EDI__Process__c proc;
        try {
            proc = [SELECT Id,EDI__Records_To_Process__c FROM EDI__Process__c WHERE Name = 'SNDINV' LIMIT 1];
        } catch (Exception e) {
            throw new EDI810OBException('Process SNDINV has not been setup');
        }
        String query = 'SELECT Id FROM EDI__Event__c '+
                       'WHERE EDI__Process__r.Name = \'SNDINV\' '+
                       'AND EDI__Consumed__c = false '+
                       'AND EDI__Skipped__c = false '+
                       'AND EDI__Process__r.EDI__Active__c = true '+
                       'AND EDI__TP_Process__r.EDI__Active__c = true '+
                       'ORDER BY CreatedDate DESC ';
        if (proc != null && proc.EDI__Records_To_Process__c != null)
            query += ' LIMIT ' + proc.EDI__Records_To_Process__c;
        return (List<EDI__Event__c>)Database.query(query);
    }
    /**
     * @description This method is called for each batch of records. Batches of records tend to execute in the order in which they’re received from the start method. It Accepts BatchableContext object and List of EDI events as parameters. It will invoke EDI.EDIService.processRecords function.
     *
     * @param Database.BatchableContext.
     * @param List of EDI events.
     * @return void.
     */ 
    global void execute(Database.BatchableContext bc,List<EDI__Event__c> events) {
        List<Schema.sObjectField> eventFields = EDI__Event__c.sObjectType.getDescribe().fields.getMap().values();
        String query = 'SELECT ' + eventFields.get(0);
        for (Integer i=1;i<eventFields.size();i++)
            query += ',' + eventFields.get(i);
        query += ',EDI__Process__r.Name,EDI__Trading_Partner__r.Name,EDI__Trading_Partner__r.EDI__Sender_ID__c,EDI__Document__r.Name';
        query += ' FROM EDI__Event__c WHERE Id IN :events FOR UPDATE ';
        try {
            events = (List<EDI__Event__c>)Database.query(query);
        } catch (Exception e) {
            errMessages.add(new KNDY4.Notify.Message('One or more records in this batch is already being processed',''));
            return;
        }
        for (Integer i=0;i<events.size();i++) {
            EDI__Event__c event = events.get(i);
            if (event.EDI__Consumed__c || event.EDI__Skipped__c) {
                events.remove(i);
                i--;
            }
        }
       
        if (events.size() > 0) {
            Savepoint sp = Database.setSavepoint();
            try {
                List<Task> exceptions = EDI.EDIService.processRecords(events,'SNDINV');              
                for(Task exTask : exceptions) {
                    if(exTask.Status.equalsIgnoreCase('Closed')) {
                        exTask.Status  = 'Completed';
                    }
                }
                EdiExceptionUtility.upsertExceptions(exceptions);                
            } catch (Exception e) {
                System.debug(e.getMessage() + e.getStackTraceString());
                errMessages.add(new KNDY4.Notify.Message(e.getMessage(),e.getStackTraceString()));
                Database.rollback(sp);
            }
        }
        errMessages.addAll(messages);
    }
    /**
     * @description This method is used to send confirmation emails or execute post-processing operations. It is called after all batches are processed. It Accepts BatchableContext object as parameter.
     *
     * @param Database.BatchableContext.
     * @return void.
     */
    global void finish(Database.BatchableContext bc) {
        KNDY4.Notify.sendEmailBatchFinish(errMessages.size(), 'EDI Outbound 810 job has completed',errMessages,bc);
        KNDY4.MasterControlProgram.finishedBatch(this);
    }
    private static KNDY4.ExceptionServiceAPI exceptionServiceApi = new KNDY4.ExceptionServiceApi();
    private static Map<Id, Map<String, Task>> mp_OpenTasks = new Map<Id, Map<String, Task>>();
    private static Map<Id, KNDY4__Customer_Invoice__c> mp_VaildKndy4CustomerInvoice = new Map<Id, KNDY4__Customer_Invoice__c>();
    private static List<NewEdiInvoice> lst_NewEdiInvoice = new List<NewEdiInvoice>();
    private static Map<Id, List<KNDY4__Customer_Invoice_Line__c>> kndy4CustomerInvoiceLineMap = new Map<Id, List<KNDY4__Customer_Invoice_Line__c>>();
    /**
     * @author HiBiZ Solutions
     * @date 02/16/2016
     * @description Validate Class created for validating the Outbound customer invoice and customer invoice lines by Implementing the EDIService.EDIStep.
     */
    global class Validate implements EDI.EDIService.EDIStep {
    private Map<Id, EDI__Event__c> mp_UpdateEdiEventMap = new Map<Id, EDI__Event__c>();
        /**
         * @description Accepts list of EDI events as parameter and executes by performing validations on customer invoice and customer invoice lines. It updates List of exceptions. Also fetches valid events , which passes all the validations.
         *
         * @param List EDI events.
         * @return EDI.EDIService.Response.
         */
        global EDI.EDIService.Response execute(List<EDI__Event__c> ediEventList) {
            EDI.EDIService.Response response = new EDI.EDIService.Response();
            List<Id> kndy4CustomerInvoiceIdList = new List<Id>();
            List<Id> taskIdList = new List<Id>();
            Map<Id, KNDY4__Customer_Invoice__c> kndy4CustomerInvoiceMap = new Map<Id, KNDY4__Customer_Invoice__c>();
            for (EDI__Event__c ediEvent : ediEventList) {
                if (ediEvent.EDI__Object_ID__c != null)
                    kndy4CustomerInvoiceIdList.add(ediEvent.EDI__Object_ID__c);
                 taskIdList.add(ediEvent.Id);
            }
            String columns = '';
            columns += 'Id, Name, KNDY4__Customer_Purchase_Order__c, KNDY4__Status__c, ';
            columns += 'KNDY4__Early_Payment_Discount_Amount__c, KNDY4__Bill_To_Customer__c,KNDY4__Bill_To_Customer__r.Name, KNDY4__Memo__c, KNDY4__Date_Invoiced__c, KNDY4__Autonumber__c, ';
            columns += 'KNDY4__Sales_Order__c, KNDY4__Sales_Order__r.KNDY4__Customer_Purchase_Order__c, KNDY4__Invoice_Type__c, KNDY4__Company__c, ';
            columns += 'KNDY4__Bill_to_Street__c, KNDY4__Bill_to_City__c, KNDY4__Bill_to_State__c, KNDY4__Bill_to_Zip_Code__c, KNDY4__Bill_to_Country__c, ';
            columns += 'KNDY4__Payment_Term__c, KNDY4__Early_Payment_Discount_Date__c, KNDY4__Due_Date__c, KNDY4__Ship_Date__c, KNDY4__Total_Amount__c ,';
            columns += 'KNDY4__Sales_Order__r.KNDY4__Customer_Purchase_Order_Date__c, KNDY4__Sales_Order__r.KNDY4__Total_Order_Volume__c, KNDY4__Sales_Order__r.KNDY4__Ship_to__c, KNDY4__Sales_Order__r.KNDY4__Ship_to__r.Name,';
            columns += 'KNDY4__Sales_Order__r.KNDY4__Ship_to__r.KNDY4__Street__c, KNDY4__Sales_Order__r.KNDY4__Ship_to__r.KNDY4__City__c , KNDY4__Sales_Order__r.KNDY4__Ship_to__r.KNDY4__State__c, KNDY4__Sales_Order__r.KNDY4__Ship_to__r.KNDY4__Zip_Code__c, KNDY4__Sales_Order__r.KNDY4__Ship_to__r.KNDY4__Country__c , ';
            columns += 'KNDY4__Payment_Term__r.KNDY4__Discount_Percent__c , KNDY4__Payment_Term__r.KNDY4__Discount_Days__c , KNDY4__Payment_Term__r.KNDY4__Due_Days__c , KNDY4__Payment_Term__r.KNDY4__Description__c , ';
            columns += 'KNDY4__Company__r.Name , KNDY4__Company__r.Remit_To_Street__c , KNDY4__Company__r.Remit_To_City__c , KNDY4__Company__r.Remit_To_State__c , KNDY4__Company__r.Remit_To_Zip__c, KNDY4__Company__r.Remit_To_Country__c , KNDY4__Transaction_Currency__c ,Terms_Type__c , Carrier_Tans_Method_Code__c , FOB_Pay_Code__c , Terms_Basis_Date_Code__c , FOB_Location_Qualifier__c  , KNDY4__Sales_Order__r.KNDY4__Total_Weight__c';
            
            //, Invoice_Number__c - col removed from select
            kndy4CustomerInvoiceMap = EDIUtility_Static.getKndy4CustomerInvoiceMap(columns, kndy4CustomerInvoiceIdList); 
            kndy4CustomerInvoiceLineMap = EDIUtility_Static.getKndy4CustomerInvoiceLine(' Id, Name, KNDY4__Quantity_Shipped__c, KNDY4__Sales_Order__c, KNDY4__Selling_UOM__c, KNDY4__Unit_Price__c, KNDY4__Shipment__c,KNDY4__Shipment_Line__c, KNDY4__Sales_Order_Line__c, KNDY4__Product__c, KNDY4__Line__c, KNDY4__Customer_Invoice__c ,KNDY4__Product__r.KNDY4__Description__c , KNDY4__Product__r.KNDY4__Selling_UOM__r.Name , KNDY4__Product__r.KNDY4__UPC_Code__c , KNDY4__Product__r.KNDY4__Weight_UOM__c , KNDY4__Product__r.KNDY4__Weight_UOM__r.Name , KNDY4__Shipment__r.KNDY4__Total_Shipment_Weight__c , KNDY4__Sales_Order_Line__r.Buyer_Part_Number__c , KNDY4__Sales_Order_Line__r.Vendor_Part_Number__c ',kndy4CustomerInvoiceIdList);
            mp_OpenTasks = EDIUtility_Static.getTasks(taskIdList, 'Open');
            for (EDI__Event__c event:ediEventList) {
                Boolean isValid = true;  
                if (!kndy4CustomerInvoiceMap.containsKey(event.EDI__Object_ID__c)) {
                     EdiExceptionUtility.setProcessException(exceptionServiceApi,event,'OB810_001');
                     isValid = false;
                    continue;
                } 
                else
                {
                    if (mp_OpenTasks.containsKey(event.Id) && mp_OpenTasks.get(event.Id).containsKey('OB810_001'))
                    {
                        EdiExceptionUtility.clearProcessException(exceptionServiceApi,event,'OB810_001');
                    }
                }
                KNDY4__Customer_Invoice__c invoice = kndy4CustomerInvoiceMap.get(event.EDI__Object_ID__c); 
                if(invoice.KNDY4__Status__c != null && invoice.KNDY4__Status__c.equalsIgnoreCase('Cancelled'))  
                {
                    skipEvent(event, 'Customer Invoice is cancelled');
                    continue;
                } 
                if (invoice.KNDY4__Sales_Order__c == null) {
                    EdiExceptionUtility.setProcessException(exceptionServiceApi,event,'OB810_002');
                    isValid = false;
                    continue;
                }
                else
                {
                    if (mp_OpenTasks.containsKey(event.Id) && mp_OpenTasks.get(event.Id).containsKey('OB810_002'))
                    {
                        EdiExceptionUtility.clearProcessException(exceptionServiceApi,event,'OB810_002');
                    }
                }
                if (invoice.KNDY4__Customer_Purchase_Order__c == null) {
                    EdiExceptionUtility.setProcessException(exceptionServiceApi,event,'OB810_003');
                    isValid = false;
                    continue;
                } 
                else
                {
                    if (mp_OpenTasks.containsKey(event.Id) && mp_OpenTasks.get(event.Id).containsKey('OB810_003'))
                    {
                        EdiExceptionUtility.clearProcessException(exceptionServiceApi,event,'OB810_003');
                    }
                }              
                if (invoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__c == null) {
                    EdiExceptionUtility.setProcessException(exceptionServiceApi,event,'OB810_004');
                    isValid = false;
                    continue;
                }
                else {
                    if (mp_OpenTasks.containsKey(event.Id) && mp_OpenTasks.get(event.Id).containsKey('OB810_004'))
                    {
                        EdiExceptionUtility.clearProcessException(exceptionServiceApi,event,'OB810_004');
                    }
                }
                if(!kndy4CustomerInvoiceLineMap.containsKey(invoice.Id))
                {
                    EdiExceptionUtility.setProcessException(exceptionServiceApi,event,'OB810_005');
                    isValid = false;
                    continue;
                }
                else
                {
                    if (mp_OpenTasks.containsKey(event.Id) && mp_OpenTasks.get(event.Id).containsKey('OB810_005'))
                    {
                        EdiExceptionUtility.clearProcessException(exceptionServiceApi,event,'OB810_005');
                    }
                }
                String customerInvoiceLineName = '';
                if (kndy4CustomerInvoiceLineMap.containsKey(invoice.Id) && kndy4CustomerInvoiceLineMap.get(invoice.Id).size() > 0) {
                    for (KNDY4__Customer_Invoice_Line__c kndy4CustomerInvoiceLine:kndy4CustomerInvoiceLineMap.get(invoice.Id)) {
                        if(kndy4CustomerInvoiceLine.KNDY4__Product__c == null)
                        {
                            if(customerInvoiceLineName != '')
                            customerInvoiceLineName += ',';
                            customerInvoiceLineName += kndy4CustomerInvoiceLine.Name ;
                        }
                    }
                    if(customerInvoiceLineName != '')
                    {
                        EdiExceptionUtility.setOtherException(exceptionServiceApi,event,'OB810_007','Product missing  for: ' + customerInvoiceLineName,'Product missing  for: ' + customerInvoiceLineName);
                        isValid = false;
                        continue;
                    }
                    else
                    {
                        if (mp_OpenTasks.containsKey(event.Id) && mp_OpenTasks.get(event.Id).containsKey('OB810_007'))
                        {
                            EdiExceptionUtility.clearOtherException(exceptionServiceApi,event,'OB810_007','Product missing','Product missing');
                        }
                    }
                }                             
                if(isValid)
                    validEdieventList.add(event);
                mp_VaildKndy4CustomerInvoice.put(event.Id,invoice);
            }           
            update ediEventList;
            return new EDI.EDIService.Response();
            //response.records = ediEventList;
            //return response;
             /*EDI.EDIService.Response response = new EDI.EDIService.Response();
             response.records = ediEventList;
             response.exceptions = exceptionServiceApi.finish(true);
             return response;*/
        }
        /**
         * @description Accepts a edi event object as parameter, marks the event as skipped 
         *  
         * @param  EDI__Event__c 
         * @param  String 
         * @return void
         *
         */
        private void skipEvent(EDI__Event__c ediEvent, String skipDetail)
        {
            ediEvent.EDI__Skipped__c = true;
            ediEvent.EDI__Skipped_Detail__c = skipDetail;
        }
    }
    /**
     * @author HiBiZ Solutions
     * @date 02/16/2016
     * @description Class created for extracting EDI invoice and EDI invoice line from valid customer invoice and customer invoice lines by Implementing the EDIService.EDIStep.
     */
    global class Extract implements EDI.EDIService.EDIStep 
    {
        Map<String, List<EDI__Invoice__c>> processedEdiInvoices = new Map<String, List<EDI__Invoice__c>>();
        Map<Id, Map<Id, EDI__Trading_Partner_Entity__c>> tpEntityMap = new Map<Id, Map<Id, EDI__Trading_Partner_Entity__c>>();
        /**
         * @description Accepts List of EDI Events as parameter and updates them into ERP system and create new EDI Invoice, EDI Invoice Lines based on the input customer Invoice and customer invoice lines.
         * 
         * @param List of EDI Event.
         * @return EDI.EDIService.Response
         */
        global EDI.EDIService.Response execute(List<EDI__Event__c> ediEventList)
        {     
            List<Id> tpIdList = new List<Id>();
            List<Id> kndy4CustomerIdList = new List<Id>();
            List<Id> kndy4SalesOrderIdList = new List<Id>();
            List<String> customerInvoiceNameList = new List<String>();
            for (EDI__Event__c ediEvent : validEdieventList)
            {
               tpIdList.add(ediEvent.EDI__Trading_Partner__c);
               if (mp_VaildKndy4CustomerInvoice.containsKey(ediEvent.Id))
               {
                  KNDY4__Customer_Invoice__c kndy4CustomerInvoice = mp_VaildKndy4CustomerInvoice.get(ediEvent.Id);
                  customerInvoiceNameList.add(kndy4CustomerInvoice.Name);
                  kndy4CustomerIdList.add(kndy4CustomerInvoice.KNDY4__Bill_To_Customer__c);
                  if(kndy4CustomerInvoice.KNDY4__Sales_Order__c != null && kndy4CustomerInvoice.KNDY4__Sales_Order__r != null){
                      kndy4SalesOrderIdList.add(kndy4CustomerInvoice.KNDY4__Sales_Order__c);
                      kndy4CustomerIdList.add(kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__c);
                  }
               }
            }
            for (EDI__Trading_Partner_Entity__c ediTradingPartnerEntity : [Select Id, Name, EDI__Ship_To_Customer__c, EDI__Trading_Partner__c From EDI__Trading_Partner_Entity__c Where EDI__Ship_To_Customer__c In : kndy4CustomerIdList And EDI__Trading_Partner__c In : tpIdList]) {
                if (! tpEntityMap.containsKey(ediTradingPartnerEntity.EDI__Trading_Partner__c)){
                  tpEntityMap.put(ediTradingPartnerEntity.EDI__Trading_Partner__c, new Map<Id, EDI__Trading_Partner_Entity__c>());
                }
                tpEntityMap.get(ediTradingPartnerEntity.EDI__Trading_Partner__c).put(ediTradingPartnerEntity.EDI__Ship_To_Customer__c, ediTradingPartnerEntity);
                
            }  
            processedEdiInvoices = getProcessedEdiInvoice('SNDINV' , '810' , customerInvoiceNameList);
            for (EDI__Event__c ediEvent : validEdieventList)
            {
                try
                {
                    KNDY4__Customer_Invoice__c kndy4CustomerInvoice = null;
                    if (mp_VaildKndy4CustomerInvoice.containsKey(ediEvent.Id))
                    {
                        kndy4CustomerInvoice = mp_VaildKndy4CustomerInvoice.get(ediEvent.Id);                        
                        EDI__Invoice__c ediInvoiceHeader = constructEdiInvoiceFromKndy4CustomerInvoice(kndy4CustomerInvoice, ediEvent); 
                        ediInvoiceHeader.EDI__Sender_ID__c = ediEvent.EDI__Trading_Partner__r.EDI__Sender_ID__c;
                        ediInvoiceHeader.EDI__Trading_Partner__c  = ediEvent.EDI__Trading_Partner__c;
                        ediInvoiceHeader.EDI__TP_Process__c  = ediEvent.EDI__TP_Process__c;
                        NewEdiInvoice insertEdiInvoice = new NewEdiInvoice();               
                        insertEdiInvoice.lst_EdiInvoiceLine = new List<EDI__Invoice_Line__c>(); 
                        Decimal totalQuantity = 0;
                        Decimal custInvoiceTotal = 0;  
                        List<KNDY4__Customer_Invoice_Line__c> kndy4CustomerInvoiceLineList = null;
                        if (kndy4CustomerInvoiceLineMap.containsKey(kndy4CustomerInvoice.Id))
                        {
                            kndy4CustomerInvoiceLineList = kndy4CustomerInvoiceLineMap.get(kndy4CustomerInvoice.Id);
                        }
                        if (kndy4CustomerInvoiceLineList != null && kndy4CustomerInvoiceLineList.size() >0)
                        {                             
                            for (KNDY4__Customer_Invoice_Line__c kndy4CustomerInvoiceLine : kndy4CustomerInvoiceLineList)
                            {
                                if((kndy4CustomerInvoiceLine.KNDY4__Quantity_Shipped__c != 0) || ((kndy4CustomerInvoiceLine.KNDY4__Quantity_Shipped__c == 0)))
                                {
                                    EDI__Invoice_Line__c ediInvoiceLine = constructEdiInvoiceLineFromKndy4CustomerInvoiceLine(ediEvent, kndy4CustomerInvoiceLine);                               
                                    insertEdiInvoice.lst_EdiInvoiceLine.add(ediInvoiceLine); 
                                }
                            }
                        }
                        ediInvoiceHeader.EDI__Total_Quantity__c = kndy4CustomerInvoice.KNDY4__Total_Amount__c;
                        ediInvoiceHeader.EDI__Customer_Invoice_Total__c = kndy4CustomerInvoice.KNDY4__Total_Amount__c;
                        insertEdiInvoice.ediInvoice = ediInvoiceHeader;
                        insertEdiInvoice.relatedEvent = ediEvent;                                        
                        lst_NewEdiInvoice.add(insertEdiInvoice);
                        ediEvent.EDI__Consumed__c = true;
                    }
                    if (mp_OpenTasks.containsKey(ediEvent.Id) && mp_OpenTasks.get(ediEvent.Id).containsKey('OB810_008'))
                    {
                        EdiExceptionUtility.clearOtherException(exceptionServiceApi, ediEvent, 'OB810_008', 'Unhandled Exception', 'Unhandled Exception');
                    }
                }
                catch (Exception iException)
                {
                    System.debug(iException.getMessage());
                    ediEvent.EDI__Consumed__c = false;
                    EdiExceptionUtility.setOtherException(exceptionServiceApi, ediEvent, 'OB810_008', 'Unhandled Exception', iException.getMessage() +' --> ' + iException.getStackTraceString());
                    messages.add(new KNDY4.Notify.Message(iException.getMessage(), iException.getStackTraceString()));
                }
             }
             try
             {
                 if (lst_NewEdiInvoice.size() > 0) 
                 {
                    insertEDIInvoice(lst_NewEdiInvoice);
                 }
                 if (validEdieventList.size() > 0)
                 {
                    Database.update(validEdieventList, true);
                 }
             }
             catch (Exception e)
             {
                messages.add(new KNDY4.Notify.Message(e.getMessage(), e.getStackTraceString()));
             }
             EDI.EDIService.Response response = new EDI.EDIService.Response();
             response.exceptions = exceptionServiceApi.finish(false); 
             return response;
        }
        /**
        * @description Accepts List of NewEdiInvoice class as parameter and inserts newly created EDI Invoice and EDI Invoice Lines.
        *
        * @param List of NewEdiInvoice.
        * @return void.
        */
        private void insertEDIInvoice(List<NewEdiInvoice> insertList)
        {
            List<EDI__Invoice__c> ediInvoiceList = new List<EDI__Invoice__c>();
            List<EDI__Invoice_Line__c> ediInvoiceLineList = new List<EDI__Invoice_Line__c>();
            Map<Id, EDI__Invoice__c> ediInvoiceCreatedMap = new Map<Id, EDI__Invoice__c>();
            Map<Id, EDI__Invoice__c> ediInvoiceDeleteMap = new Map<Id, EDI__Invoice__c>();
            Map<Id, EDI__Event__c> ediInvoiceEventMap = new Map<Id, EDI__Event__c>();
            Map<Id, EDI__Event__c> ediEventUpdatedMap = new Map<Id, EDI__Event__c>();
            Map<Id, EdiEventError> ediEventErrorMap = new Map<Id, EdiEventError>();
            for (NewEdiInvoice ediInvoice : insertList) 
            {
                ediInvoiceList.add(ediInvoice.ediInvoice);
            }
            Integer i = 0;
            for (Database.SaveResult saveResult : Database.insert(ediInvoiceList, false))
            {
                NewEdiInvoice newInvoice = insertList.get(i);
                if (saveResult.isSuccess())
                {
                    ediInvoiceCreatedMap.put(newInvoice.ediInvoice.Id, newInvoice.ediInvoice);
                    ediInvoiceEventMap.put(newInvoice.ediInvoice.Id, newInvoice.relatedEvent);
                    if (mp_OpenTasks.containsKey(newInvoice.relatedEvent.Id) && mp_OpenTasks.get(newInvoice.relatedEvent.Id).containsKey('OB810_009'))
                    {
                        EdiExceptionUtility.clearOtherException(exceptionServiceApi, newInvoice.relatedEvent, 'OB810_009', ' Failed to insert new edi invoice ', '');
                    }
                    for (EDI__Invoice_Line__c ediInvoiceLine : newInvoice.lst_EdiInvoiceLine)
                    {
                        ediInvoiceLine.EDI__Invoice__c = newInvoice.ediInvoice.Id;
                    }
                    ediInvoiceLineList.addAll(newInvoice.lst_EdiInvoiceLine);
                }
                else
                {
                    newInvoice.relatedEvent.EDI__Consumed__c = false;
                    for (Database.Error databaseError : saveResult.getErrors())
                    {
                        EdiExceptionUtility.setOtherException(exceptionServiceApi, newInvoice.relatedEvent, 'OB810_009', ' Failed to insert new edi invoice ', databaseError.getStatusCode() + ' : ' + databaseError.getMessage()+' Edi Invoice fields that caused this error: ' + databaseError.getFields());
                        break;
                    }
                }
                i++;
            }
            if (ediInvoiceLineList.size() > 0)
            {
                i = 0;
                for (Database.SaveResult saveResult : Database.insert(ediInvoiceLineList, false))
                {
                    EDI__Invoice__c ediInvoice = ediInvoiceCreatedMap.get(ediInvoiceLineList.get(i).EDI__Invoice__c);
                    EDI__Event__c ediEvent = ediInvoiceEventMap.get(ediInvoiceLineList.get(i).EDI__Invoice__c);
                    EdiEventError error = null;
                    if(ediEventErrorMap.containsKey(ediEvent.Id))
                    {
                        error = ediEventErrorMap.get(ediEvent.Id);
                    }
                    else
                    {
                        error = new EdiEventError();
                        error.errorList = new List<Database.Error>();
                        error.ediEvent = ediEvent;
                        ediEventErrorMap.put(ediEvent.Id, error);
                    }
                    if ( ! saveResult.isSuccess()) 
                    {
                        if (! ediInvoiceDeleteMap.containsKey(ediInvoice.Id))
                        {
                            ediInvoiceDeleteMap.put(ediInvoice.Id, ediInvoice);
                        }
                        for (Database.Error databaseError : saveResult.getErrors())
                        {
                            error.errorList.add(databaseError);
                            break;
                        }
                    }
                    i++;
                }
                for(Id ediEventId : ediEventErrorMap.keySet())
                {
                    EdiEventError error = ediEventErrorMap.get(ediEventId);
                    EDI__Event__c ediEvent = error.ediEvent;
                    if(error.errorList.size() > 0)
                    {
                        ediEvent.EDI__Consumed__c = false;
                        Database.Error databaseError = error.errorList[0];
                        EdiExceptionUtility.setOtherException(exceptionServiceApi, ediEvent, 'OB810_010', ' Failed to insert new edi invoice line', databaseError.getStatusCode() + ' : ' + databaseError.getMessage()+' Edi Invoice Line fields that caused this error: ' + databaseError.getFields());
                    }
                    else
                    {
                        if (mp_OpenTasks.containsKey(ediEvent.Id) && mp_OpenTasks.get(ediEvent.Id).containsKey('OB810_010'))
                        {
                            EdiExceptionUtility.clearOtherException(exceptionServiceApi, ediEvent, 'OB810_010', ' Failed to insert new edi invoice line', '');
                        }
                    }
                }
            }
            if(ediInvoiceDeleteMap.size() > 0)
            {
                Database.delete(ediInvoiceDeleteMap.values(), false);// SaveResult not required here.
            }         
         }
        /**
         * @description Accepts Customer Invoice and EDI Event as parameters and constructs EDI Invoice header based on the Customer Invoice header informations.
         *
         * @param Customer Invoice.
         * @param EDI Event.
         * @return EDI Invoice Header.
         */
        public EDI__Invoice__c constructEdiInvoiceFromKndy4CustomerInvoice(KNDY4__Customer_Invoice__c  kndy4CustomerInvoice, EDI__Event__c ediEvent)
        {
            KNDY4__Customer__c kndy4Customer = null;
            KNDY4__Sales_Order__c kndy4SalesOrder = null;
            KNDY4__Payment_Term__c kndy4PaymentTerm = null;
            KNDY4__Shipment__c kndy4Shipment = null; 
            List<KNDY4__Customer_Invoice_Line__c> kndy4CustomerInvoiceLineList = null;
            EDI__Trading_Partner_Entity__c btTpEntity;           
            EDI__Trading_Partner_Entity__c stTpEntity;           
            if (tpEntityMap.containsKey(ediEvent.EDI__Trading_Partner__c)){
               Map<Id, EDI__Trading_Partner_Entity__c> entityMap = tpEntityMap.get(ediEvent.EDI__Trading_Partner__c);
               if (!entityMap.isEmpty()){
                  if (kndy4CustomerInvoice.KNDY4__Bill_To_Customer__c != null){
                    if (entityMap.containsKey(kndy4CustomerInvoice.KNDY4__Bill_To_Customer__c)){
                       btTpEntity = entityMap.get(kndy4CustomerInvoice.KNDY4__Bill_To_Customer__c); 
                    }                 
                 }                 
                  if(kndy4CustomerInvoice.KNDY4__Sales_Order__c != null && kndy4CustomerInvoice.KNDY4__Sales_Order__r != null && kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__c != null)
                  {
                      if (entityMap.containsKey(kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__c)){
                        stTpEntity = entityMap.get(kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__c);
                      }
                  }
               }               
            } 
            if (kndy4CustomerInvoiceLineMap.containsKey(kndy4CustomerInvoice.Id))
            {
                kndy4CustomerInvoiceLineList = kndy4CustomerInvoiceLineMap.get(kndy4CustomerInvoice.Id);
            }
            EDI__Invoice__c ediInvoiceHeader = new EDI__Invoice__c();
            ediInvoiceHeader.Name = EDIUtility_Static.constructObjectName();
            ediInvoiceHeader.EDI__Document_ID__c = '810';
            //ediInvoiceHeader.EDI__Message_ID__c = ediEvent.EDI__Trading_Partner__r.EDI__Sender_ID__c + '_' + ediEvent.Name + '_' + ediInvoiceHeader.Name;//ediEvent.Id;
            ediInvoiceHeader.EDI__Process_Name__c = 'SNDINV';          
            ediInvoiceHeader.EDI__Process_Flag__c = 'U';
            ediInvoiceHeader.EDI__Invoice_Date__c = kndy4CustomerInvoice.KNDY4__Date_Invoiced__c.date();
            ediInvoiceHeader.EDI__Invoice__c = kndy4CustomerInvoice.Name;
            ediInvoiceHeader.EDI__Invoice_Total__c = kndy4CustomerInvoice.KNDY4__Total_Amount__c;
            if (processedEdiInvoices != null && processedEdiInvoices.containsKey(kndy4CustomerInvoice.Name)){
                ediInvoiceHeader.EDI__Purpose_Code__c = 'U5';
            }else if(ediInvoiceHeader.EDI__Invoice_Total__c > 0){
                ediInvoiceHeader.EDI__Purpose_Code__c = 'DR';
            }
            else{
                ediInvoiceHeader.EDI__Purpose_Code__c = 'CR';
            }
            ediInvoiceHeader.EDI__Event__c = ediEvent.Id;
            if(kndy4CustomerInvoice.KNDY4__Sales_Order__c != null && kndy4CustomerInvoice.KNDY4__Sales_Order__r != null)
            {
                kndy4SalesOrder = kndy4CustomerInvoice.KNDY4__Sales_Order__r;
                ediInvoiceHeader.EDI__Sales_Order__c = kndy4SalesOrder.Id;
                ediInvoiceHeader.EDI__PO_Date__c = kndy4SalesOrder.KNDY4__Customer_Purchase_Order_Date__c;
                ediInvoiceHeader.EDI__PO_Number__c = kndy4SalesOrder.KNDY4__Customer_Purchase_Order__c;
                ediInvoiceHeader.EDI__Total_Volume__c = kndy4SalesOrder.KNDY4__Total_Order_Volume__c;
                ediInvoiceHeader.EDI__Total_Weight__c = kndy4SalesOrder.KNDY4__Total_Weight__c;
                
            }
            if (kndy4CustomerInvoice.KNDY4__Payment_Term__c != null)
            { 
                kndy4PaymentTerm = kndy4CustomerInvoice.KNDY4__Payment_Term__r;
                ediInvoiceHeader.EDI__ITD03_Disc_Percent__c = kndy4PaymentTerm.KNDY4__Discount_Percent__c;
                ediInvoiceHeader.EDI__ITD04_Disc_Due_Date__c = kndy4CustomerInvoice.KNDY4__Early_Payment_Discount_Date__c;
                ediInvoiceHeader.EDI__ITD05_Disc_Days_Due__c = kndy4PaymentTerm.KNDY4__Discount_Days__c;
                ediInvoiceHeader.EDI__ITD06_Net_Due_Date__c = kndy4CustomerInvoice.KNDY4__Due_Date__c;
                ediInvoiceHeader.EDI__ITD07_Net_Days__c = kndy4PaymentTerm.KNDY4__Due_Days__c;
                ediInvoiceHeader.EDI__ITD12_Description__c = kndy4PaymentTerm.KNDY4__Description__c;         
            }
            if(kndy4CustomerInvoice.KNDY4__Company__c != null)
            {
                ediInvoiceHeader.EDI__N101_ID1__c = 'RI';
                ediInvoiceHeader.EDI__N103_Qualifier1__c = '9';
                ediInvoiceHeader.EDI__N102_Name1__c = kndy4CustomerInvoice.KNDY4__Company__r.Name;
                ediInvoiceHeader.EDI__N301_Address1_1__c = EDIUtility_Static.getStreetAddressPart1(kndy4CustomerInvoice.KNDY4__Company__r.Remit_To_Street__c);
                ediInvoiceHeader.EDI__N302_Address1_2__c = EDIUtility_Static.getStreetAddressPart2(kndy4CustomerInvoice.KNDY4__Company__r.Remit_To_Street__c);
                ediInvoiceHeader.EDI__N401_City1__c = kndy4CustomerInvoice.KNDY4__Company__r.Remit_To_City__c;
                ediInvoiceHeader.EDI__N402_State1__c = kndy4CustomerInvoice.KNDY4__Company__r.Remit_To_State__c;
                ediInvoiceHeader.EDI__N403_ZipCode1__c = kndy4CustomerInvoice.KNDY4__Company__r.Remit_To_Zip__c;
                if(kndy4CustomerInvoice.KNDY4__Company__r.Remit_To_Country__c.equalsIgnoreCase('United States'))
                    ediInvoiceHeader.EDI__N404_Country1__c = 'USA';
            }
            if (kndy4CustomerInvoice.KNDY4__Bill_To_Customer__c != null)
            {
                ediInvoiceHeader.EDI__N101_ID2__c = 'BT';
                if (btTpEntity != null){
                  ediInvoiceHeader.EDI__N104_Entity2__c = btTpEntity.Name;
                }
                ediInvoiceHeader.EDI__N103_Qualifier2__c = '09';
                ediInvoiceHeader.EDI__N102_Name2__c = kndy4CustomerInvoice.KNDY4__Bill_To_Customer__r.Name;
                ediInvoiceHeader.EDI__N301_Address2_1__c = EDIUtility_Static.getStreetAddressPart1(kndy4CustomerInvoice.KNDY4__Bill_to_Street__c);
                ediInvoiceHeader.EDI__N302_Address2_2__c = EDIUtility_Static.getStreetAddressPart2(kndy4CustomerInvoice.KNDY4__Bill_to_Street__c);
                ediInvoiceHeader.EDI__N401_City2__c = kndy4CustomerInvoice.KNDY4__Bill_to_City__c;
                ediInvoiceHeader.EDI__N402_State2__c = kndy4CustomerInvoice.KNDY4__Bill_to_State__c;
                if (kndy4CustomerInvoice.KNDY4__Bill_to_Zip_Code__c != null)
                {
                    ediInvoiceHeader.EDI__N403_ZipCode2__c = kndy4CustomerInvoice.KNDY4__Bill_to_Zip_Code__c.replaceAll('-','');
                }
                //if(kndy4CustomerInvoice.KNDY4__Bill_to_Country__c != null && kndy4CustomerInvoice.KNDY4__Bill_to_Country__c.length() <=10)   
                    //ediInvoiceHeader.EDI__N404_Country2__c = kndy4CustomerInvoice.KNDY4__Bill_to_Country__c;
                if(kndy4CustomerInvoice.KNDY4__Bill_to_Country__c != null && kndy4CustomerInvoice.KNDY4__Bill_to_Country__c.equalsIgnoreCase('United States'))
                    ediInvoiceHeader.EDI__N404_Country2__c = 'USA';
            }
            if (kndy4CustomerInvoice.KNDY4__Sales_Order__c != null &&   kndy4CustomerInvoice.KNDY4__Sales_Order__r != null 
                && kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__c != null && kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__r != null)
            {
                kndy4Customer = kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__r;
                ediInvoiceHeader.EDI__N101_ID3__c = 'ST';
                if (stTpEntity != null){
                  ediInvoiceHeader.EDI__N104_Entity3__c = stTpEntity.Name;
                }
                ediInvoiceHeader.EDI__N103_Qualifier3__c = '09';
                ediInvoiceHeader.EDI__N102_Name3__c = kndy4CustomerInvoice.KNDY4__Sales_Order__r.KNDY4__Ship_to__r.Name;
                String street = kndy4Customer.KNDY4__Street__c;
                ediInvoiceHeader.EDI__N301_Address3_1__c = EDIUtility_Static.getStreetAddressPart1(street);
                ediInvoiceHeader.EDI__N302_Address3_2__c = EDIUtility_Static.getStreetAddressPart2(street);
                ediInvoiceHeader.EDI__N401_City3__c = kndy4Customer.KNDY4__City__c;
                ediInvoiceHeader.EDI__N402_State3__c = kndy4Customer.KNDY4__State__c;
                if (kndy4Customer.KNDY4__Zip_Code__c != null)
                {
                    ediInvoiceHeader.EDI__N403_ZipCode3__c = kndy4Customer.KNDY4__Zip_Code__c.replaceAll('-','');
                } 
                //if(kndy4Customer.KNDY4__Country__c!= null && kndy4Customer.KNDY4__Country__c.length() <=10)     
                    //ediInvoiceHeader.EDI__N404_Country3__c = kndy4Customer.KNDY4__Country__c;
                if(kndy4Customer.KNDY4__Country__c != null && kndy4Customer.KNDY4__Country__c.equalsIgnoreCase('United States'))
                    ediInvoiceHeader.EDI__N404_Country3__c  = 'USA';
            }
            ediInvoiceHeader.Buyer_Currency__c  = kndy4CustomerInvoice.KNDY4__Transaction_Currency__c;
            ediInvoiceHeader.EDI__DTM_Date1__c = kndy4CustomerInvoice.KNDY4__Ship_Date__c;
            ediInvoiceHeader.EDI__DTM_Qualifier1__c = '011';
            ediInvoiceHeader.EDI__ITD08_Terms_Disc__c  = kndy4CustomerInvoice.KNDY4__Early_Payment_Discount_Amount__c;
            ediInvoiceHeader.EDI__ITD01_Terms_Type__c  = kndy4CustomerInvoice.Terms_Type__c.substring(0,2);
            ediInvoiceHeader.EDI__ITD02_Basis__c = kndy4CustomerInvoice.Terms_Basis_Date_Code__c.substring(0,2);
            ediInvoiceHeader.EDI__FOB_Point__c  = kndy4CustomerInvoice.FOB_Pay_Code__c.substring(0,2);
            ediInvoiceHeader.EDI__Transportation_Method__c  = kndy4CustomerInvoice.Carrier_Tans_Method_Code__c.substring(0,2);
            ediInvoiceHeader.FOB_Location_Qualifier__c  = kndy4CustomerInvoice.FOB_Location_Qualifier__c.substring(0,2);
            return ediInvoiceHeader;
        }
        /**
         * @description Accepts process name, document id, and list of invoice names as parameters and determines the list of EDI Invoices associated with this customer invoice.
         *
         * @param String
         * @param String.
         * @param List of String.         
         * @return Map<String, List<EDI__Invoice__c>>.
         * 
         */
        private  Map<String, List<EDI__Invoice__c>> getProcessedEdiInvoice(String processName,String documentId , List<String> invoiceNameList)
        {
            Map<String, List<EDI__Invoice__c>> invoiceMapByCiName = new Map<String, List<EDI__Invoice__c>>();
            for (EDI__Invoice__c ediInvoice : [Select Id, Name, EDI__Process_Name__c, EDI__Document_ID__c, EDI__Invoice__c, EDI__Process_Flag__c From EDI__Invoice__c WHERE EDI__Process_Name__c =: processName and EDI__Document_ID__c  =: documentId and EDI__Invoice__c In: invoiceNameList and EDI__Process_Flag__c = 'P'])
            {
                if (invoiceMapByCiName.containsKey(ediInvoice.EDI__Invoice__c)){
                    invoiceMapByCiName.get(ediInvoice.EDI__Invoice__c).add(ediInvoice);
                }else{
                    invoiceMapByCiName.put(ediInvoice.EDI__Invoice__c, new List<EDI__Invoice__c>{ediInvoice});
                }                
            }
            return invoiceMapByCiName;
        }
        /**
         * @description Accepts EDI Event and Customer Invoice Line as parameters and extracts the EDI Invoice Line informations.
         *
         * @param EDI Event.
         * @param Customer Invoice Line.
         * @return EDI Invoice Line.
         */
        public EDI__Invoice_Line__c constructEdiInvoiceLineFromKndy4CustomerInvoiceLine(EDI__Event__c ediEvent, KNDY4__Customer_Invoice_Line__c kndy4CustomerInvoiceLine)
        {
            EDI__Invoice_Line__c ediInvoiceLine = new EDI__Invoice_Line__c(); 
            if (kndy4CustomerInvoiceLine.KNDY4__Line__c != null)
            {
                ediInvoiceLine.EDI__Line_Number__c = EDIUtility_Static.leftPad('' + Integer.valueOf(kndy4CustomerInvoiceLine.KNDY4__Line__c), '0', 3);
            }
            ediInvoiceLine.EDI__Invoiced_Quantity__c = kndy4CustomerInvoiceLine.KNDY4__Quantity_Shipped__c;
            ediInvoiceLine.EDI__Invoiced_Price__c = kndy4CustomerInvoiceLine.KNDY4__Unit_Price__c.setScale(4);
            if(kndy4CustomerInvoiceLine.KNDY4__Product__c != null)
            {
                if(kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__Description__c != null)
                    ediInvoiceLine.EDI__Item_Description__c = kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__Description__c ;
                if(kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__Selling_UOM__r.Name != null)
                {
                    ediInvoiceLine.EDI__Invoiced_UOM__c = kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__Selling_UOM__r.Name;
                    ediInvoiceLine.EDI__Customer_UOM__c = kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__Selling_UOM__r.Name;     
                }
                ediInvoiceLine.Weight_UOM__c =  kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__Weight_UOM__r.Name; 
                if(kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__UPC_Code__c != null)
                {
                    ediInvoiceLine.EDI__Qualifier1__c = 'UP';
                    ediInvoiceLine.EDI__Alias1__c = kndy4CustomerInvoiceLine.KNDY4__Product__r.KNDY4__UPC_Code__c;
                }
            }
            ediInvoiceLine.EDI__Customer_Quantity__c = ediInvoiceLine.EDI__Invoiced_Quantity__c;
            ediInvoiceLine.EDI__Customer_Price__c = ediInvoiceLine.EDI__Invoiced_Price__c.setScale(4);
            System.debug('kndy4CustomerInvoiceLine.KNDY4__Sales_Order_Line__r - '+kndy4CustomerInvoiceLine.KNDY4__Sales_Order_Line__c);
            ediInvoiceLine.Buyer_Part_Number__c = kndy4CustomerInvoiceLine.KNDY4__Sales_Order_Line__r.Buyer_Part_Number__c; // Kndy4SalesOrderLine.Buyer_Part_Number__c;  
            ediInvoiceLine.Vendor_Part_Number__c =  kndy4CustomerInvoiceLine.KNDY4__Sales_Order_Line__r.Vendor_Part_Number__c;//Kndy4SalesOrderLine.Vendor_Part_Number__c;
            return ediInvoiceLine;
        }
       
    }
   /**
    * @description Accepts two parameter event and  List of EDI Events and retrun event list having same object ID 
    *
    * @param List<EDI__Event__c>
    * @param EDI__Event__c
    * @return List<EDI__Event__c> list of related event based on the object Id
    */
    private static List<EDI__Event__c> getOtherEventsForSameObject(EDI__Event__c ediEvent, List<EDI__Event__c> ediEventList)
    {
        List<EDI__Event__c> eventList = new List<EDI__Event__c>();
        Iterator<EDI__Event__c> ediEventIterator = ediEventList.iterator();
        while(ediEventIterator.hasNext())
        {
            EDI__Event__c event = ediEventIterator.next();
            if (ediEvent.Id != event.Id && ediEvent.EDI__Object_ID__c == event.EDI__Object_ID__c)
            {
                eventList.add(event);
            }
        }
        return eventList;
    }       
    /**
     * @author HiBiZ Solutions
     * @date 02/16/2016
     * @description Class used for storing Newly created EDI Invoice and EDI Invoice Lines.
     *
     */
    private class NewEdiInvoice 
    {
        public EDI__Invoice__c ediInvoice {get; set;}
        public List<EDI__Invoice_Line__c> lst_EdiInvoiceLine {get; set;}     
        public EDI__Event__c relatedEvent {get; set;}   
    }   
    /**
     * @author HiBiZ Solutions
     * @date 02/16/2016
     * @description EdiEventError Class created for storing EDI Event Id and Error List. 
     *
     */
    private class EdiEventError
    {
        public List<Database.Error> errorList {get; set;}
        public EDI__Event__c ediEvent {get; set;}        
    }
}