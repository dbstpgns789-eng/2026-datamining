# drafts/Jeminana — Code Review / 코드 리뷰

| | Result / 결과 | Verdict / 판정 |
|---|---|---|
| [Q1](Q1.ipynb) | `772` | ✅ Correct / 정답 |
| [Q2](Q2.ipynb) | `93` | ✅ Correct / 정답 |
| [Q3](Q3.ipynb) | 29 countries / 29개국 | ✅ Fixed / 수정 완료 |

---

## Q1 — 2-head word frequency / 앞 두 글자 빈도

**How it works / 코드 설명**

| Step | Code | EN | KR |
|---|---|---|---|
| 1 | `sc.textFile("sherlock.txt")` | Read the book line by line | 책을 한 줄씩 읽기 |
| 2 | `flatMap(lambda line: line.split())` | Split lines into words | 줄을 단어로 나누기 |
| 3 | `filter(lambda word: len(word) > 2)` | Keep words longer than 2 letters | 길이가 2보다 긴 단어만 남기기 |
| 4 | `map(lambda word: word.lower()[:2])` | Lowercase, take the first 2 letters | 소문자로 바꾸고 앞 두 글자 가져오기 |
| 5 | `map(lambda word: (word, 1))` | Make each one a pair `(head, 1)` | 각각 `(앞 두 글자, 1)` 쌍으로 만들기 |
| 6 | `reduceByKey(lambda a, b: a + b)` | Add up the 1s → count per head | 같은 키끼리 더해서 빈도 세기 |
| 7 | `filter(lambda x: x[1] < 32)` | Keep heads that appear less than 32 times | 빈도가 32 미만인 것만 남기기 |
| 8 | `map(lambda x: x[1]).sum()` | Add up those counts → **772** | 남은 빈도를 모두 더하기 → **772** |

**Review / 리뷰**
- **EN:** Correct, and every step uses Spark operations. Small cleanup: `small_counts.values().sum()` is shorter than `.map(lambda x: x[1]).sum()`.
- **KR:** 정답이며 모든 단계를 Spark 연산으로 처리했습니다. `.map(lambda x: x[1]).sum()` 대신 `small_counts.values().sum()`으로 더 간단하게 쓸 수 있습니다.

---

## Q2 — Integer pair statistics / 정수 쌍 통계

**How it works / 코드 설명**

| Step | Code | EN | KR |
|---|---|---|---|
| 1 | `sc.textFile("int-pair-input.txt")` | Read lines like `"(2, 96)"` | `"(2, 96)"` 같은 줄 읽기 |
| 2 | `map(lambda line: tuple(map(int, line.strip("()").split(", "))))` | Remove `()`, split, turn into numbers → `(2, 96)` | 괄호 제거 후 나누고 숫자로 변환 → `(2, 96)` |
| 3 | `pairs.first()[0]` | First element of the first pair → **2** | 첫 번째 쌍의 첫 원소 → **2** |
| 4 | `map(lambda x: x[0])` + `max()` / `min()` | Largest / smallest first element → **32** / **0** | 첫 원소의 최댓값 / 최솟값 → **32** / **0** |
| 5 | `filter(lambda x: x[0] == 1).count()` | How many pairs start with 1 → **31** | 첫 원소가 1인 쌍의 개수 → **31** |
| 6 | `filter(lambda x: x[0] == 2).count()` | How many pairs start with 2 → **28** | 첫 원소가 2인 쌍의 개수 → **28** |
| 7 | `first_value + max_value + ...` | Add the five numbers → **93** | 다섯 값을 더하기 → **93** |

**Review / 리뷰**
- **EN:** Correct. `split(", ")` only works if there is exactly one space after the comma. `split(",")` is safer, because `int()` ignores spaces.
- **KR:** 정답입니다. `split(", ")`는 쉼표 뒤 공백이 정확히 한 칸일 때만 동작합니다. `int()`가 공백을 무시하므로 `split(",")`이 더 안전합니다.

---

## Q3 — Happiness score never went down / 행복지수가 떨어지지 않은 나라

**How it works / 코드 설명**

| Step | Code | EN | KR |
|---|---|---|---|
| 1 | `print(year, rdd.first())` | Print each year's column names to check them | 연도별 컬럼 이름을 출력해서 확인하기 |
| 2 | `map(lambda line: next(csv.reader([line])))` | Split each CSV line into fields (handles commas inside quotes) | CSV 줄을 필드로 나누기 (따옴표 안의 쉼표도 처리) |
| 3 | `filter(lambda row: row[0] != "Country")` | Drop the header row | 헤더 줄 제거 |
| 4 | `map(lambda row: (row[0], round(float(row[2]), 3)))` | Keep `(country, score)`, rounded to 3 decimals | `(국가, 점수)`만 남기고 소수점 셋째 자리로 반올림 |
| 5 | `scores[2015].join(scores[2016])...` | Join the 5 years → only countries in every year remain | 5개 연도를 join → 모든 연도에 있는 국가만 남음 |
| 6 | `x[1][0][0][0][0], ...` | Unpack the nested join result into 5 scores | 중첩된 join 결과를 점수 5개로 풀기 |
| 7 | `x[1][0] <= x[1][1] <= ... <= x[1][4]` | Keep countries whose score never went down | 점수가 한 번도 떨어지지 않은 국가만 남기기 |
| 8 | `map(lambda x: x[0]).collect()` | Get the country names → **29 countries** | 국가 이름만 가져오기 → **29개국** |

**Review / 리뷰**

The original answer had 28 countries. Two fixes bring it to 29.
원래 답은 28개국이었고, 두 가지를 수정해 29개국이 되었습니다.

1. **Burundi was missing / Burundi 누락**
   - **EN:** The 2017 file stores 2.905 as `2.904999971`, so `2.905 <= 2.904999971` was `False`. Rounding scores to 3 decimals (step 4) fixes this.
   - **KR:** 2017년 파일에 2.905가 `2.904999971`로 저장되어 있어 `2.905 <= 2.904999971`이 `False`였습니다. 점수를 소수점 셋째 자리로 반올림(4단계)해서 해결했습니다.
2. **CSV parsing / CSV 파싱**
   - **EN:** `split(",")` breaks on `"Hong Kong S.A.R., China"`. `csv.reader` (step 2) reads that row correctly.
   - **KR:** `split(",")`는 `"Hong Kong S.A.R., China"` 행을 잘못 나눕니다. `csv.reader`(2단계)는 이 행을 올바르게 읽습니다.

**Note / 참고**
- **EN:** Taiwan, Trinidad and Tobago, and Macedonia are named differently in some years, so `join` drops them. Names are kept as given, because the question says the countries differ each year.
- **KR:** Taiwan, Trinidad and Tobago, Macedonia는 연도별로 이름이 달라 `join`에서 제외됩니다. 문제에서 매년 조사 국가가 다르다고 했으므로 이름은 그대로 두었습니다.
