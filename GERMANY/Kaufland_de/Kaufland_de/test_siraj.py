import requests

cookies = {
    'AB-optimizely_user': '38df72dd-1d54-451a-ba58-3c10a250211c',
    'AB-optimizely__device_type': 'desktop',
    'AB-optimizely__browser_name': 'Chrome',
    'AB-optimizely__environment': 'production',
    'ALTSESSID': 'jpn7q8pgm65jm50tiiaptsndd2',
    'OptanonAlertBoxClosed': '2024-10-02T07:57:31.480Z',
    'eupubconsent-v2': 'CQF2nyQQF2nyQAcABBENBJF4APLAAAAAAAYgF5wBAAKgAoABYAvMAAAAgSAEABUBeY6AEABUBeZKACAvMpACAAqAvMAA.flgAAAAAAAAA',
    '_gcl_au': '1.1.1729977423.1727855852',
    '_fbp': 'fb.1.1727855851866.767702107317593549',
    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%228iR20Kh4li1Q6t1bnNRP%22%7D',
    '_ga': 'GA1.1.1925035489.1727855853',
    'axd': '4375020065483650037',
    'tis': '',
    'FPAU': '1.1.1729977423.1727855852',
    'api_ALTSESSID': 'jpn7q8pgm65jm50tiiaptsndd2',
    'x-storefront': 'de',
    'storefront-selector-preferences': '[]',
    'hm_tracking': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.MmU5ZDg1M2QwZjY3YjE5ZmYxOTkzNjlmOGNkNjIyOWEyYzY1YzIwNWM3ODNiYzkwNDY0NjA2ZTU0YzFhNTE0Ng%3D%3D.bEmlaXzfRN4cL%2F5r7PWss1b3kAwOUwYVfcyNCjbBfyI%3D',
    '_ttp': 'cwZxOBfl18hAUCjK6eQ5mjgnB1y',
    'kndctr_BCF65C6655685E857F000101_AdobeOrg_identity': 'CiY0ODY2NDA0ODY2MzgzOTc2NjI5MDM4MTk4MTc5Mjc4Nzc1MjM3OVIRCNndv5S2MhgBKgRJUkwxMAHwAdndv5S2Mg==',
    'AMCV_BCF65C6655685E857F000101%40AdobeOrg': 'MCMID|48664048663839766290381981792787752379',
    'mbox': 'session%2348664048663839766290381981792787752379%2DKpfDXH%231732527281',
    'lea_utms': 'utm_source=stationary-de-header&utm_medium=referral&utm_campaign=stationary-de-header&utm_content=undefined',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Mon+Nov+25+2024+14%3A40%3A24+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d4d9c87c-f278-41dd-9bef-a9b22e5cbcdf&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1%2CC0072%3A0&geolocation=IN%3BGJ&AwaitingReconsent=false&isAnonUser=1',
    '_uetsid': '169e0840ab0c11ef961fc703dc109fa9',
    '_uetvid': 'fa6b99d0809311efa4d1a3be79b17001',
    '_ga_9WNMNEZ2M0': 'GS1.1.1732525387.4.1.1732525882.0.0.986990383',
    'cf_clearance': '0fXM9Zry4P5ugbOpAIuBOneFHTGQmmdaFVHFlXn9x7c-1732528186-1.2.1.1-JTnGx1fOmKEn4Ez.KcGGnhJYFXqb10yfbmVvcIRxDPkg3qPLqXdvN2XirJG6OkK9roGTDBfoN4_hESHg59CqXyExLZwGLqNhcYGfr5fn5zkk7O857u9vOTY4RKU.YSzr_JbdxWTRo9HbyscdKShR_qETHx9eTkDRVm98qenRs0t_RC52EbGgtHJuXCh6gIx2RGsM5ARpd9TNWMsS08Y9_9GyzCmJqrg3JRo4gnmzTAjPVqUq07RHqFB2mUn2VMtOzZxmSljoixM9rFWtQ5wh2cZCV31Y4xL37X62n85A_rFKSztk0Miy7AAaAGaB8ZTzqwethxkvYTq2AOd_X8W9nw3FVTS5wVdz6MRgc0PkA9A4d.pey9bu6NaPLn7xr8wQgif0tQDx8qIiRY7Jtjr4Y8NfUX0ItH3CMC0DGn.CeAj56AZ_JThmqxMX5UbKtjXY',
    'x-country': 'DE',
    '_dd_s': 'isExpired=1',
    '__cf_bm': 'SFFuxj38CINiiUWmI_NoSJ41VUw_DEsddiJxx0d2Fww-1732529499-1.0.1.1-GM9RCsAxzSisSow4ERtzNFSoe7oElYQiOlymlRxT1BjcLHIUL2wtaTz9kZxH37sv_xvUWARf6NIbzVPO967jAQ',
    'cf_chl_rc_i': '1',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'content-type': 'application/x-www-form-urlencoded',
    # 'cookie': 'AB-optimizely_user=38df72dd-1d54-451a-ba58-3c10a250211c; AB-optimizely__device_type=desktop; AB-optimizely__browser_name=Chrome; AB-optimizely__environment=production; ALTSESSID=jpn7q8pgm65jm50tiiaptsndd2; OptanonAlertBoxClosed=2024-10-02T07:57:31.480Z; eupubconsent-v2=CQF2nyQQF2nyQAcABBENBJF4APLAAAAAAAYgF5wBAAKgAoABYAvMAAAAgSAEABUBeY6AEABUBeZKACAvMpACAAqAvMAA.flgAAAAAAAAA; _gcl_au=1.1.1729977423.1727855852; _fbp=fb.1.1727855851866.767702107317593549; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%228iR20Kh4li1Q6t1bnNRP%22%7D; _ga=GA1.1.1925035489.1727855853; axd=4375020065483650037; tis=; FPAU=1.1.1729977423.1727855852; api_ALTSESSID=jpn7q8pgm65jm50tiiaptsndd2; x-storefront=de; storefront-selector-preferences=[]; hm_tracking=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.MmU5ZDg1M2QwZjY3YjE5ZmYxOTkzNjlmOGNkNjIyOWEyYzY1YzIwNWM3ODNiYzkwNDY0NjA2ZTU0YzFhNTE0Ng%3D%3D.bEmlaXzfRN4cL%2F5r7PWss1b3kAwOUwYVfcyNCjbBfyI%3D; _ttp=cwZxOBfl18hAUCjK6eQ5mjgnB1y; kndctr_BCF65C6655685E857F000101_AdobeOrg_identity=CiY0ODY2NDA0ODY2MzgzOTc2NjI5MDM4MTk4MTc5Mjc4Nzc1MjM3OVIRCNndv5S2MhgBKgRJUkwxMAHwAdndv5S2Mg==; AMCV_BCF65C6655685E857F000101%40AdobeOrg=MCMID|48664048663839766290381981792787752379; mbox=session%2348664048663839766290381981792787752379%2DKpfDXH%231732527281; lea_utms=utm_source=stationary-de-header&utm_medium=referral&utm_campaign=stationary-de-header&utm_content=undefined; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Nov+25+2024+14%3A40%3A24+GMT%2B0530+(India+Standard+Time)&version=202410.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d4d9c87c-f278-41dd-9bef-a9b22e5cbcdf&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CBG2530%3A1%2CC0030%3A1%2CBG2520%3A1%2CBG2487%3A1%2CBG2488%3A1%2CBG2489%3A1%2CBG2490%3A1%2CBG2491%3A1%2CBG2492%3A1%2CBG2493%3A1%2CC0053%3A1%2CC0049%3A1%2CC0054%3A1%2CC0041%3A1%2CC0047%3A1%2CC0055%3A1%2CC0072%3A0&geolocation=IN%3BGJ&AwaitingReconsent=false&isAnonUser=1; _uetsid=169e0840ab0c11ef961fc703dc109fa9; _uetvid=fa6b99d0809311efa4d1a3be79b17001; _ga_9WNMNEZ2M0=GS1.1.1732525387.4.1.1732525882.0.0.986990383; cf_clearance=0fXM9Zry4P5ugbOpAIuBOneFHTGQmmdaFVHFlXn9x7c-1732528186-1.2.1.1-JTnGx1fOmKEn4Ez.KcGGnhJYFXqb10yfbmVvcIRxDPkg3qPLqXdvN2XirJG6OkK9roGTDBfoN4_hESHg59CqXyExLZwGLqNhcYGfr5fn5zkk7O857u9vOTY4RKU.YSzr_JbdxWTRo9HbyscdKShR_qETHx9eTkDRVm98qenRs0t_RC52EbGgtHJuXCh6gIx2RGsM5ARpd9TNWMsS08Y9_9GyzCmJqrg3JRo4gnmzTAjPVqUq07RHqFB2mUn2VMtOzZxmSljoixM9rFWtQ5wh2cZCV31Y4xL37X62n85A_rFKSztk0Miy7AAaAGaB8ZTzqwethxkvYTq2AOd_X8W9nw3FVTS5wVdz6MRgc0PkA9A4d.pey9bu6NaPLn7xr8wQgif0tQDx8qIiRY7Jtjr4Y8NfUX0ItH3CMC0DGn.CeAj56AZ_JThmqxMX5UbKtjXY; x-country=DE; _dd_s=isExpired=1; __cf_bm=SFFuxj38CINiiUWmI_NoSJ41VUw_DEsddiJxx0d2Fww-1732529499-1.0.1.1-GM9RCsAxzSisSow4ERtzNFSoe7oElYQiOlymlRxT1BjcLHIUL2wtaTz9kZxH37sv_xvUWARf6NIbzVPO967jAQ; cf_chl_rc_i=1',
    'origin': 'https://www.kaufland.de',
    'priority': 'u=0, i',
    'referer': 'https://www.kaufland.de/api/search/v1/result-product-offers?requestType=initial-load&pageType=search&searchValue=spirituosen&deviceType=desktop&useNewUrls=true&__cf_chl_tk=M5qXkSfMsg7qxThYdvKXMVyX7GNGAQ7R9s3pWmhlC5c-1732529501-1.0.1.1-T_Z63X7MaVi8Ozu_NqfZ7kBJD6FjNkWZVnhBAnK2dmA',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"131.0.6778.86"',
    'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.86", "Chromium";v="131.0.6778.86", "Not_A Brand";v="24.0.0.0"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"15.0.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

params = {
    'requestType': 'initial-load',
    'pageType': 'search',
    'searchValue': 'spirituosen',
    'deviceType': 'desktop',
    'useNewUrls': 'true',
}

data = {
    'f91a2626c311fbff88109428c8b6f65c946a5649147874a535a627dde56eb1f8': 'rp8XpNQ4NLaaMb4r42M3cbbsTAJnxBqh31g9JdUvq2I-1732529501-1.2.1.1-f787mLuXf2hPOW__S14rzhASyubkIbAUL8al3nlPpPDbD.vj.Z9EaejBscakbc7_1fT7DetxpTgyPXQrOd8BCfDC8RTR1yVs5cTs4tSR_f_cj9JuYYiOltGCmfgKTxX9.9q30Vou7vt7BehvDqvZqiXfYdw3Gb42bbwvZcBI3VDo2mJjW4JwSPQ.J3yQtYHtwh6fUqr78X.BOFYOSNQ75zU4hiQNz5diihLLlQ9v7H0hi24qtolhipWf54OQJTd1VgyNaS5D7ljLFDFykoko_dC3D7k8J4PDBjBf7z6DNQzfjLNSSJecrCKTXBC66CBBgYDkY0VPOW_NtlPssR71DkGwQQKPVJV_wHIOwRPzoC0Z6vmFodSYVHeOrjYs9xsa16_uhsyiYuwyYrH6_zz3xe2IlnxFcq6mMqOG0qLU5TrToAxO79Tu.fGLZ6dL42GCERzQ_JfldFMYEs_MvXLKHgsMgdWnrP0pbLEDLExbfseCuLSdofFoNONENIXj6OYEEZ6zWDdmn8U.i231xJ3G_Q_5lz_o.c6O56N5VDU.Xumg7bDI1B987rV5uLMcgErjIHxMMAB.fsdhwTs2tMAHmoVsaLj7hl_FvZJppRc8obIpIw6GgHgbryK9QOR3WmESLyE0JNlszTn9ULIUWwj9KQIYRjDNUkJOcpksZIfXM_Jf4H5NmU6LgKi.YqDfWhpkNDEZNyihmaUYXlxfcMyKqtA3qOSgkUfNuKurpNSXMAf0AbcwPeEqJhkMmOvo6zj7JJxFVmvrfRBTDHtXilvs48V7QFqoGgRgOnDR0EY6GIrhFfQ_GnJkSNcZ6MB9D_T0WNoN2v.y.DfYkR6uzPSj3oCdhyr8IZeR.Et.n2m3JrssqV4bbTm22b_MHdEvDFKfTs878eBX3yFPJc7oBxBHIwcvoWnnuLAht_4cigyHI59CVlZ_3_1OiqKemqtSTdaHYw3sdehDsEvDLDYWPaUoI0DC6JRft8J03iLqIVtBNbcSeXD9lDbpJ9OYr.qFAO2f5Xd0DsXL0b_eUz88hhAARs._ljsGa9KJx7F.hyMVUhAhP88R1bboLpMM_vxUvVu7vebtWJCiUYnne6194SeF6.ADEeLSGeaKjgSFb88miiioKYqp5nZoH01C2zb9a5PRLxayImGwElqZIsglKstdjPxZrZFczHrQuWceXVNsat7LSeec2DqhLcVDiI6UXldod6Q.yMnCYYAODuUEg3Fu5L8_WbLJQK9C9N2QygLLhjjwd0J.c4i6qhMGglofUXXW256q2J2hYVGRsL0ZT1vl5KqrbQWsebb12lfJdLlr8Xqk5dW4lPx7GMAz02VAM6YNB4X6Pnq3xrXodM40MBBNxnJvj8t.0MGrAFR.xVeH4PqwupEGr8kN.SHFwlCfoYw7qnLsdlczs05kWO4Fca0BfPnFBWZaTG4pBaI7KHZHGYAbJ3HTR50xAL.YfXiargy2GIk.Os4fo3mCo4O6pqeqL0kdCvZYZNTGyfbFmNY_YYQKa63XAGJwrnjBJ3CrEHLXqUhvpGofZLh3a4Gj.qV59DxXcVtV.lFhsgZpxFYjhG1J1F5ljevJIVSMTVmu20acBPr7jzHCQIZbrCRGEQyEGDebXeMtiAXju2fhDhJKW8q7lbVvjgLnehjgjEEuuhc4pGV6GHvPHAfDWUf.5oEO7HSzhk1s2WFpIy2btaZVNp4RtslnnZG30rvU4Z4ibnZ9TJjMKHiczSD5H5V2yCZSrpqUEkOlmwkjrCY49vK7tEaVp61pD5LgNhbdqyPJKiWscFhJciIIe2xeY2wqJhI_mAC8UTCiBQ7ubwcTNeDfliyoo6AGIa4xpTO3n.Z2ILPJ9hjskdMsiVO4gwQ5oEtGUXibnZLtbJvZ4sYE93qwPPO_aqV9SlDelSqzMLzZynEyIZlT9LTo0jvcwptAJe_3hyPftCN3H5T7UJSKCj1EOodnScka8mLCd1VDd9R1uOADJfAtNNCE7.1skWRbQj7gFKpbI0QoEm9WCCzS0b70ARAgpeFI0F1I2y_LjWuCbdJHfFiQhMaj9hqGxLaFWp01bFcidzuehdIlBWG.5cbOchTZpauUyJtphbXR0m2JPUAV5w4CpTM3rthuPQVHBmcc7QUUY_R6Ubdy41QYO24WTAIP2RfYuMbhtwrY030.nXPv.woEm.4a0.xrfBIatDfC7nUZr7reOrNqw3FBeBfPMAnGDQnC9_.tUffcUhgnxQilh1GVoykz5wEVJyjwl7zJ4Jz_Zr39AiSGQXVmsaQ2fVkMcdHPs0m57oK30CRsvDSefp3ymmhXAqMLpefZ4EpXwc9f9WHDNEvlCW8Jd5QRwvg',
    'dcdd5d984d21579da9710e13b85f26119bc6b992e2d1529549bb88c39b939be4': 'QkQx6UZ_wfHs3r18IddpiqbVbtK2I7d3MXV2qUbj8ko-1732529501-1.2.1.1-PKd4aOO7P27eulv2DcVZP05wVUY9tSbFxJEIVZv5g2IRHxaNazO9eX9XdkpFofrY6OnN.zPNuLp56Uqxkls7Dq8KAZJaJtMxJk_v4l2OP_C_iZcJBgj2eaWBQJOOEgbhmSnYu_mlYOZ9RNEX8oiW3Eut2L7zKnShEHCXU97penNSDwo5zjInv3WC.QnBWWMPBvVmcQSivHhVuWo9NGX4Q.jAs94pmi20q._BQNTN4Nl795zVa9jG3By1Jn6O1ZJtwH8gBVUJ8i.LAluR248.DCED7xfdAvftHqbn7syJtd1MgZe80Gfz0eR0EQpcnmRISX_GX3Ai.Q4j2numkwR4Cb98O5q__9foQ9A_zdxXTRZtHPBXzVFB.PaJYq9b7O58RzGqAlhrMUh2XWqjBMYhickVWAIptiV9RZ3Qpkw9M5xBy_EY0_0RlRUzMBUxNcAa_ppIB1XPSDpQ7pARO5nPw.F.ydGSuFVR079RBhvk9VOOH9rBkiLJEs64J97305xbdSaVuimfMEw65I9HgrEBnEY13N6t79e9rjnBFszLZ53H2zNTJa5D5XKJt4DLJe7mKwoYHxYYNH3q_Jsqaz7.dit1B_bzh8ykMMoFgOu5DQCr8CyKpnBfQELu9SnVowVTdxP22P0MqPNXenlMyXZidZAq5sVglqdAKwPO.J5fcj.do21_oFOkIAyd_WnvX9IxdkDpqBMYsKuRIGLOf_PusUBioc9IEQCyeWMC.1yiR18kMOQ54Hl3EQjXLd3sACXJXMgBixBZBoh31mMoL5tZDor47.SLTuRrQ8aDaB9.j8V1C1rQy06lCOEPrckz91HIpiK923QyryPULE1VABrwKSu3JcY_45HpHBykU5fQRKtsBczRKRLvHt1fAGsrP9YXu8IW8npb.A9RtbGqqiYVP5PXj_kNCQqw3HCGz.B0iY6sBeDfFbkl42JlEvbePBfaEDRj.eny_FU1zVdhgGtb5t0qW0iGreM.aRyPlQzGwNnSKZSlThuVfG5FV00gz7ld5SW66MFvmjp2Qw7FBv9R3ee0_aG.HsKTh2XdPLqE_FkYvmQJ80QcuuYCt_kSDKCTj9p7hVHCSRJTBgGE9GPvSuBhpa2oVO22pxyNVGByBOYJCg20OajoXSdX7hbbIwmfDDxcGWuMQdN2wr7nDK3yDlpiNb8L_9.Xo1WnW8.ah7l9BMtXrRTLoTc.dj7HIkCe0stwtHnS3ApTN7u9m8.NcNg0cyYEqZ23n2yHnziP23d4rcE9F.oyJ9dWlG7kAyrfmiwgjymqxRcDmiHnuQ.uYwNUEzBu9bi5iehtFTeLrADVJWcV9IunT33Ee2AxTSQFrAErCIwOL0ex8s9Ew8YEFTTh5hOhXp2GI6pslfhuLz2oZ1Uet3WY4ftlGCzWiBsFqbfGWjl9SQBtUE90tNLr5sRzoQ8WZ8kLpP0errHuTU32XNCSOuOUgbzVBegWhxWO2O2ke88AzKs2lcUnzEAkjO0xpxJ0gTv8kcVd2ZUzd5fnDV.stNbEj8TcirJJUT42L9C4rBwHmZheyUXLDM6At6fYhQJTZoNWWks51KmaEswYJhNfpTGY6WrWtZzs28A0cqKv4hWnaeqcL7mtJUjZO3wh7Wbe2okPCynF2BJEnM2HyhcekNjuTYYRwJSlUjhNAmicwXFIlupXse3f9R2SQ36V1197j5b5l1KO0Wd1V4cnlzSHT6K0pwGgZrRQt6EOJSRxGAJxqRDXJWdWNy4PijV.hSgFK6_o5OUFxLT36pqZKOAF75UeFJITRP6F7sCcKQ.YF2jqX6it.m7xlJDPQ7H7jWt1zlhuUom4umGnLycsSOBq2KULCsGwoa9fmK1UPxBgmYlY9GgSACkTQJkQvf.nn743NlpF9nxCPtttfAvkUx5.yHBZbWENfGVhrWEQsUqVsCtVpN9ZaWUW3JqNPcuL_0IFfbKq6WMAZP7w49BsahWPLe4NpiSWnKtlw9Qimn5IYQMjzNe2.VbgVyYgqkevC.Y4yaqAdx6pinmDY0qwv2UFdCF8aX2H8pGFvFcVeS2pMZh1fGl80hpDfyEaEjM0OG3e4MVUUABeQTlOg4.mnCX4N6YPoCvoFXv1LEXFDlmAAatDckgXU4hp6AOQbMLCUeICDQ3ZjCi.LCmCAovkNckYu1UtPJ.U3._SToI9PisGHGH5lc3LLyviV1o0bIsbxcoi_0MvCQgjzMXnVfbhDaratENw5HXOLe3w9tdUSx2STExctzzJnJJ1EEPXmymjA6KQY1YHA1auScbXWDRkpOIbvCC8QFhImcGKHSgyygOWn1P_nFpEN_HOrUTfA5BxJWf7dPPASrDRcYxdjidJXrn8J199oyEXeBQyhSO4x0R6V905J1mR8a0l_rsqQd9F0NlfTrpkPcGP0Y5LmARqhxpx0zhR_lGzD1E2p4Lf5EOD2tpMGgyWHv_xnQngKIyNicmI5tZJxUX3jKQ4gAX9dWoBv6DdqPgRv9jTF7v0k6la4JG42QQd_4viktR5kQF4VIoZVwQEc7ZBOInsPGXCX7piZEYc49pH4eyhxFbf_I4HyKln68crnSbkbYLJqBl3xDaUuAdkd7CSw3lvwgUK9kOOj72nyoW6lVyICpBhOr0dh.xzPsAmGEF8Urf398aClk8DGHrNkVZjWtpMItVTR4CY8upJn9Xf1H0Cb74Kk_eYsgBS5rSg.lgmF2osK7vhMePiK_vGEaJoINrDtU9DvvtJ.P12AQRwGrQt7bDETIlxK6ZuyqSwwYMtPGi9PQmuv.HwdcyB.SpQpYo9ewPmtxY2jO80OC5DTCUaWJpZCu.KYhRLTGSL93ziPQtZfVKrfS6_v5ucIzjZXiSeURrBuMib__yM25lY0MwnjD4iG.dYvg5WsioXnKAHbxAYBg4PZxxBMvJOYNvgjr4bCJaMZbcsToKniTB5EtsssOkWNtpoDYyoopg1kpiY33WJr5dSE1_.UzQmIE4EmKiCmgAtCATYOyiaostBcyANf.n.vWKCXmSlgthJo2mUemKHS0WHbVC_IBdSc95vNn.dMK.DNb_cmKysY9jWl18geWnz6cXE3zbCCTudlWtxjT5_qWadRm69S3GkTtmJE2tO_.0lrJnD6uEWtPG8B.0qI9eHbedzpchnH8Q57RN5Lm7Bzy53jHRDPL1fMlAp_wKTbi6kz2UBoSdSrxvXVyGPnpA7ccOmhdnaY5wcj28TIuzBV_d6fCkcsd.UAOdvwvdxDIHHoAMy.5UaYcqoxjHwbCZDye6d0.e_Xx1N2hE_JKONJPhbpsXXgNLRKTEw2Jxw6c6RWYPp3AIWXoP8CqBr4FXk5iu3q1wDpXr.3BN6PPmIwGm.sdfnWpM2BqU0fCnoUQL.mfKSAo4uG9QmswmBMikR1uBG4NmkNmv1rB5.8xcPnlnTFOKw4u2lV3A759PXTA2CJ0IGFsLjmOghzlQQ38nan1p60xz4LVUpxr27NVM.gbn458SVtoO1lpSd2nexbm4y8fCwk2qbHGOpn9pU05DDWcT_HxfUapHYg2YjFZbpNxzG3_Hlc9KIJhSSgdRWTzoukGE7JgjFDC.sfLPtzIRdwXnDAokU85v8ya1CZR1W3Veolq4NGBjG31ynA_AIO15B73ZUEJ8h3M1rbqMrZ0OTcAeYf7UMk.JZnNW0EtPTxP7dzl9UuXchSmXWLzmZs.iDaOhZnC1esl0lHhXZ4OxTt8GdpetXP6xkCdvNkSaBecN6N0RUawqiPNANVkHeL9p7fETAi3NJOaiW2rNOXBJdi4wPGXm8wTKjiJa4EYpxkGxFZgrdBCx0sAmqZpw.To9OlaudIHsBsOgKxw2nDndK.Jh555juvoceO3t8mCgZ8KK6OcNkHTbv5gbt5U.mp1z62kLBL9dfJKPSVuzFhkYGSJY_p7YHkesograhZ87413viQRcOLmzSnglvQnQATkpc_e9pkSAtR02sy0tTEi23y1ymjwU3p6Dk5rptBBLCdVhkbxeNp.pvWyVU0AWl270fLTn60lCTnSJT6QZeP.1S8Vw784.udBt07KRikl3LZNP.Jl54Q87vsgFiP_4mbgqmkehUL4vDy25_htaVtecCFqtOkGyvyfZ1T4Jom_bIQrfgozc9uFA6zp6Cfl0e6PlGVeHL0QenbPlkMtlFurddPA.G_hGNx4aDAaT0abOFeQ3jpuVyeYpYuwQxUybzAdC9FQhkFu0emtbjCvnKvX_eAuMvAOZoaUIDJqIWdN6P4EpNA1RmQ_sA6NDaY4.b4k6ZZqN2soWBy.PabD2zVVQmmM6J354jlq.5l1uvJ_JH3XBOL36sqOv5TIwRAltYn6Nvq0EnlSWyuoCrLXbM21r_3.VseYXXA4d3hPmRrttHbqOUx3CR3BSXD4tejR8zvR8bFdSfOXSflaDr1WuGkPAEY5m0cPSjc0jIf17QBqx7BW9btYcobavW5SHDs1zrlhFRsIsxe9R9Kp0m_0c30LAiu3nhY8NmIuUZUg.XlZdVTPu6.d.LUmExyV3qRDa1wv9rPcyDxDa_eFpHHj7CoZV2oEZTZ37mUTwo3Ny.pKMj.1.r6An.HPi__LSM1UHOUYK7rQvWnGndkA3.5MrKaM3mERU7dPDysmBaItaQ50CMmWjIi5FlAySqmOoHhRtG0JDHJVhYGtFVSQJrXi2at1kWY7PRcF3Z__eJoZXa6yQEyLT1wM3L1NUe_1ZMb2xW.5AWvJmOesploEt4R5v.DOaLzFtr4s0hOIvNLpIRTEb5cMe3lzHDeIUCdl45F1UtRP5dVe39GVxEjvh9cUDfK02PEAkSO5JFUZFX03QCvXhG1qrdqH6Hn_0B9UezsXuoSXMFOCdodMqoYLyk5ExLK7VFKxvmM.iKJnSjFVKPm.W4qto47zq53hdIQ2Xxj5hE8ADWOockkHmmZsmFQ2Qnms0fcYSBk99qb6gpZs2OTRu3AmQM3PLfbhb9QwAyPP9fHUFsYsX.y6xj1o5UIfiK_JaE0rqes6AG9eouKRUnnStT3yvfzVWlCSg10y5SYRmbBviC4vVCUw8.G83YtBFYgA0D',
    '00bafd47d1a8acec438bc2321b4ba01aeed906d5909bcc1ce7d6a1fd26138c8c': '9yPBY8sAr9hQilSJyjGfOSs5e4KrFAcdSbv3XAGiY0Q-1732529509-1.1.1.1-3AEZwCkETqsoHEsgQXYUyBDm2I8oRl.m8rOALkcHPGDiAOX2hQoY9KWb4qY7q.Em8iwTcv8a6HiYfVhHSSe13e6nUDEhWFbVDs7l6X7sVBIL49GNxxJcOeFxQYRhh24GUaqfDMYFAiNcSnefSgpecuWa89UCUqnQbCeKu89WhZ1oPgjlII2YoX5jXFxCWQ48',
}

response = requests.post(
    'https://www.kaufland.de/api/search/v1/result-product-offers',
    params=params,
    cookies=cookies,
    headers=headers,
    data=data,
)
print(response.text)