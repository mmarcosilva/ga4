
view: prueba_ga4 {
  derived_table: {
    sql: SELECT channel, source 
      from `bq-test-283619.analytics_167672070.LR_V7LS11733498059050_paid_video` ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  set: detail {
    fields: [
        channel,
	source
    ]
  }
}
