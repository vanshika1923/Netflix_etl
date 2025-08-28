# Data Dictionary (Music Events)

## Bronze
- bronze_artists(artist_id, name, genre, country, created_at)
- bronze_venues(venue_id, name, city, state, country, capacity)
- bronze_events(event_id, artist_id, venue_id, event_date, status, ticket_price, currency)
- bronze_customers(customer_id, name, email, city, state, country, created_at)
- bronze_tickets(ticket_id, event_id, customer_id, purchase_datetime, channel, price_paid, currency, status)
- bronze_checkins(checkin_id, event_id, ticket_id, scanned_at, gate)
- bronze_refunds(refund_id, ticket_id, event_id, reason, refund_amount, refunded_at)
- bronze_campaigns(campaign_id, event_id, channel, spend, impressions, clicks, conversions, start_date, end_date)

## Silver (cleaned)
- silver_artists(... deduped)
- silver_venues(... capacity int)
- silver_events(... enum(status), price numeric)
- silver_customers(... email normalized)
- silver_tickets(... enums(channel,status); FK‑valid only)
- silver_checkins(... FK‑valid only)
- silver_refunds(... FK‑valid only)
- silver_campaigns(... enums(channel), dates)

## Gold
- gold_event_sales(event_id, event_date, artist_id, venue_id, venue_capacity, tickets_sold, attendance, no_show_rate, gross_revenue, refunds_amount, net_revenue, sell_through)
- gold_daily_metrics(date, tickets_sold, gross_revenue, net_revenue)
- gold_campaign_roi(event_id, channel, spend, conv_tickets, conv_revenue, cpa, roi)
