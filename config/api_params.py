'''api 요청 페이로드 형식'''
def international_payload_form(first, departure, arrival, date, fare_type, galileo_key="", travel_biz_key=""):
    if first:
        galileo_flag=True
        galileo_key=""
        travel_biz_flag=False
        travel_biz_key=""
    else:
        galileo_key=galileo_key
        galileo_flag=galileo_key!=""
        travel_biz_key=travel_biz_key
        travel_biz_flag=travel_biz_key!=""
    
    payload={
            "operationName": "getInternationalList",
            "variables": {
                "adult": 1,
                "child": 0,
                "fareType": "Y",
                "galileoFlag": galileo_flag,
                "galileoKey": galileo_key, # 인증키
                "infant": 0,
                "fareType":fare_type,
                "isDirect": False, # 직항 여부
                "itinerary": [
                    {
                        "departureAirport": departure,
                        "arrivalAirport": arrival,
                        "departureDate": date
                    }
                ],
                "stayLength": "", # 편도
                "travelBizKey": travel_biz_key,
                "travelBizFlag": travel_biz_flag,
                "where": "pc",
                "trip": "OW",
            },
            "query": "query getInternationalList($trip: InternationalList_TripType!, $itinerary: [InternationalList_itinerary]!, $adult: Int = 1, $child: Int = 0, $infant: Int = 0, $fareType: InternationalList_CabinClass!, $where: InternationalList_DeviceType = pc, $isDirect: Boolean = false, $stayLength: String, $galileoKey: String, $galileoFlag: Boolean = true, $travelBizKey: String, $travelBizFlag: Boolean = true) {\n  internationalList(\n    input: {trip: $trip, itinerary: $itinerary, person: {adult: $adult, child: $child, infant: $infant}, fareType: $fareType, where: $where, isDirect: $isDirect, stayLength: $stayLength, galileoKey: $galileoKey, galileoFlag: $galileoFlag, travelBizKey: $travelBizKey, travelBizFlag: $travelBizFlag}\n  ) {\n    galileoKey\n    galileoFlag\n    travelBizKey\n    travelBizFlag\n    totalResCnt\n    resCnt\n    results {\n      airlines\n      airports\n      fareTypes\n      schedules\n      fares\n      errors\n      carbonEmissionAverage {\n        directFlightCarbonEmissionItineraryAverage\n        directFlightCarbonEmissionAverage\n      }\n    }\n  }\n}"
        }
    return payload

def domastic_payload_form(departure, arrival, date, fare_type):
    payload={
            "operationName": "domesticFlights",
            "variables": {
                "device":"PC",
                "fareType":fare_type,
                "itinerary": [
                    {
                        "departureAirport": departure,
                        "arrivalAirport": arrival,
                        "departureDate": date
                    }
                ],
                "person":{
                    "adult": 1,
                    "child": 0,
                    "infant": 0
                }
            },
            "query":"""
                query domesticFlights($itinerary: [Itinerary]!, $person: Passengers!, $fareType: SeatClass!, $device: Device!) {
                domesticFlights(
                    itinerary: $itinerary
                    person: $person
                    fareType: $fareType
                    device: $device
                ) {
                    departures {
                    airlineCode
                    airlineName
                    codeshare
                    fitName
                    depCity
                    arrCity
                    departureCityName
                    arrivalCityName
                    departureDate
                    arrivalDate
                    dayDiff
                    departureTime
                    arrivalTime
                    detailedClass
                    seatClass
                    seatCnt
                    journeyTime
                    minFare
                    supportNPay
                    fare {
                        agtCode
                        bookingClass
                        adultFare
                        childFare
                        aFuel
                        cFuel
                        aTax
                        cTax
                        publishFee
                        etc
                        supportNPay
                        discountFare {
                        adultDiscountFare
                        childDiscountFare
                        discountInfo {
                            cardType {
                            code
                            name
                            }
                            discountType
                            discountRange
                            amount {
                            unit
                            value
                            }
                        }
                        }
                    }
                    }
                    arrivals {
                    airlineCode
                    airlineName
                    codeshare
                    fitName
                    depCity
                    arrCity
                    departureCityName
                    arrivalCityName
                    departureDate
                    arrivalDate
                    dayDiff
                    departureTime
                    arrivalTime
                    detailedClass
                    seatClass
                    seatCnt
                    journeyTime
                    minFare
                    supportNPay
                    fare {
                        agtCode
                        bookingClass
                        adultFare
                        childFare
                        aFuel
                        cFuel
                        aTax
                        cTax
                        publishFee
                        etc
                        supportNPay
                        discountFare {
                        adultDiscountFare
                        childDiscountFare
                        discountInfo {
                            cardType {
                            code
                            name
                            }
                            discountType
                            discountRange
                            amount {
                            unit
                            value
                            }
                        }
                        }
                    }
                    }
                }
                promotions(route: domestic) {
                    departureStartDate
                    departureEndDate
                    paymentMethods {
                    code
                    name
                    }
                    otas
                    airlines
                    originAirports
                    destinationAirports
                    passengers {
                    adult
                    child
                    infant
                    }
                    benefit {
                    unit
                    value(route: domestic)
                    limit
                    }
                    description
                }
                }
                """
        }
    
    return payload

def return_header(is_domestic, departure, arrival, date, seat_class):
    if is_domestic:
        referer=f"https://flight.naver.com/flights/domestic/{departure}-{arrival}-{date}?adult=1&child=0&infant=0&isDirect=true&fareType={seat_class}"
    else:
        referer=f"https://flight.naver.com/flights/international/{departure}-{arrival}-{date}?adult=1&fareType={seat_class}"

    headers = {
        "authority": "airline-api.naver.com",
        "method": "POST",
        "path": "/graphql",
        "scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "no-cache",
        "content-type": "application/json",
        "origin": "https://flight.naver.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": referer,
        "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
    }
    return headers