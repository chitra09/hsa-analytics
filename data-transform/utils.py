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
