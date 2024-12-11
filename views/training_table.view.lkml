view: training_table {
  sql_table_name: `arima_demo.training_table` ;;

  dimension: conversions {
    type: number
    sql: ${TABLE}.conversions ;;
  }
  dimension: split {
    type: string
    sql: ${TABLE}.split ;;
  }
  dimension: subproducto_desc {
    type: string
    sql: ${TABLE}.Subproducto_Desc ;;
  }
  dimension_group: timely_timestamp {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    datatype: datetime
    sql: ${TABLE}.timely_timestamp ;;
  }
  measure: count {
    type: count
  }
}
