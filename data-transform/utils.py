@outputSchema("output:map[]")
def parseTransactions(input):
    output = {}
    if input is not None:
        fields = input.split("_")
        if len(fields) == 3:
            # memberId
            output['memberId'] = fields[0]

            # category
            output['category'] = fields[1]

            # paymentDate
            output['paymentDate'] = fields[2]
        
    return output;



@outputSchema("output:map[]")
def parseClaims(input):
    output = {}
    if input is not None:
        fields = input.split("_")
        if len(fields) == 3:
            # memberId
            output['memberId'] = fields[0]

            # claimId
            output['claimId'] = fields[1]

            # dateProcessed
            output['dateProcessed'] = fields[2]
        
    return output;

@outputSchema("output:map[]")
def parseMembers(input):
    output = {}
    if input is not None:
        fields = input.split("_")
        if len(fields) == 2:
            # memberId
            output['memberId'] = fields[0]

            # dependentId
            output['dependentId'] = fields[1]
        
    return output;
    




@outputSchema("output:bag{t:tuple(group::memberId: bytearray,group::year: chararray,amount: double,isMaxed: chararray)}")
def reviewMaxMin(memberId,bag):
    output          = []
   
    lastYear    = -1
    lastAmount  = -1

    for element in bag:
        listElement = list(element)

        if( element[1] == '' ):
            listElement[3] = '-'
            tup=(memberId,listElement[1],listElement[2],listElement[3])
            output.append(tup)
            continue

        #looping until first valid value
        if( lastYear < 0 ):
            lastYear    = element[1]
            lastAmount  = element[2]
            if(listElement[2] > 0):
                listElement[3] = 'T'
            
            tup=(memberId,listElement[1],listElement[2],listElement[3])
            output.append(tup)
            continue

        #If the las amount of the last year is greater then T if not F remains
        if( element[2] > lastAmount ):
            listElement[3] = 'T'

        #We grab the las value as the last year for the next compair
        lastYear    = element[1]
        lastAmount  = element[2] 
        tup=(memberId,listElement[1],listElement[2],listElement[3])
        output.append(tup)

    return output;


@outputSchema("output:chararray")
def setQuarterValues(sample):
    output          = []
    dateArray       = sample.split('-')
    month           = (int)(dateArray[1])
    if( month >= 1 and month <= 4):
        return '1'
    if( month >= 5  and month<= 8):
       return '2'
    if( month >= 9  and month<= 12):
        return '3'
    return '0';



