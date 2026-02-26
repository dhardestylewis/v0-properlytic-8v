import { createClient } from '@supabase/supabase-js'
import * as dotenv from 'dotenv'
import fs from 'fs'
dotenv.config({ path: '.env.local' })

const supabase = createClient(process.env.NEXT_PUBLIC_SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!)

async function check() {
    console.log("Checking RPC get_historical_metrics...")
    const { data, error } = await supabase.rpc('get_historical_metrics', {
        p_table: 'oppcastr.metrics_parcel',
        p_key_col: 'acct',
        p_id: '0101010101'
    })
    fs.writeFileSync('out.json', JSON.stringify({ data, error: error?.message }, null, 2))
}
check()
