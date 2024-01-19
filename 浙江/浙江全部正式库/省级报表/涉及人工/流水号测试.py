
i= {
                "330100": 1.1,
                "330200": 1

              }
for item in i.items() :
    key = item[0]
    value = item[1]

    print((key, value))

    overrate = (U_过车_站点表.total_weight - U_过车_站点表.limit_weight * value) * 100 / (U_过车_站点表.limit_weight * value)
    U_过车_站点表=U_过车_站点表[(U_过车_站点表.area_city==key)
                            &(overrate>=100)
                            &(U_过车_站点表.total_weight<100)
                            ]
    print(U_过车_站点表)

