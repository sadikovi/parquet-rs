const EPOCH_DAYS: i64 = 719162;

struct Fields {
  pub day_of_year: i64,
  pub day_of_week: i64,
  pub month: i64,
  pub year: i64
}

impl Fields {
  pub fn new() -> Self {
    Self { day_of_year: 0, day_of_week: 0, month: 0, year: 0 }
  }
}

fn convert_time(time: i64) {
  let fields = Fields::new();
  let mut day = time / (24 * 60 * 60 * 1000);
  let mut millis = time % (24 * 60 * 60 * 1000);
  if millis < 0 {
    millis += 24 * 60 * 60 * 1000;
    day -= 1;
  }

  calculate_day(fields, day, false);
}

fn calculate_day(fields: Fields, day: i64, gregorian: bool) {
  let mut year = 1970;
  if gregorian {
    year += ((day - 100) * 400) / (365 * 400 + 100 - 4 + 1);
  } else {
    year += ((day - 100) * 4) / (365 * 4 + 1);
  }

  if day >= 0 {
    year += 1;
  }

  let first_day_of_year = get_linear_day(year, 1, gregorian);
}

fn get_linear_day(year: i64, day_of_year: i64, gregorian: bool) -> i64 {
  let julian_day = (year - 1) * 365 + ((year - 1) >> 2) + (day_of_year - 1) - EPOCH_DAYS;
  if gregorian {
    let offset = ((year - 1) / 400) - ((year - 1) / 100);
    return julian_day + offset;
  } else {
    return julian_day - 2;
  }
}

fn main() {
  // Usage
  // let seconds = 1262391174;
  /*
  let seconds = -1262391174;
  let mut tm = DateTime::new();
  seconds_to_datetime(seconds, &mut tm);
  println!("{}", tm.date());
  println!("{}", tm);
  */
}
