# drafts/Jeminana — Code Review / 코드 리뷰

| | Task / 문제 | Result / 결과 | Verdict / 판정 |
|---|---|---|---|
| [Q1](Q1.ipynb) | 2-head word frequency sum (< 32) | `772` | ✅ Correct / 정답 |
| [Q2](Q2.ipynb) | Sum of 5 statistics on int pairs | `93` | ✅ Correct / 정답 |
| [Q3](Q3.ipynb) | Countries with non-decreasing happiness 2015–2019 | 28 countries | ⚠️ Misses 4 countries / 4개국 누락 |

---

## Q1 — 2-head word frequency / 앞 두 글자 빈도

### English
**Pipeline:** `textFile → flatMap(split) → filter(len > 2) → map(lower()[:2]) → (k, 1) → reduceByKey → filter(< 32) → sum`

**Good**
- Every step is a Spark transformation/action, and `.collect()` is never used, so the "Spark only" rule is satisfied.
- The length filter is applied to the raw token, before lowercasing and slicing, which is the right order.
- Punctuation is kept, which the spec allows (e.g. `(b`, `“o` appear as heads).

**Suggestions**
- `small_counts.map(lambda x: x[1]).sum()` can be shortened to `small_counts.values().sum()`.
- The `pairs` step could be merged into the previous `map`: `valid_words.map(lambda w: (w.lower()[:2], 1))`.
- The `.take(5)` debug cells are useful while developing. Consider removing them, or adding short notes, in the final version.

### 한국어
**처리 흐름:** `textFile → flatMap(split) → filter(len > 2) → map(lower()[:2]) → (k, 1) → reduceByKey → filter(< 32) → sum`

**잘한 점**
- 모든 단계를 Spark 연산으로 처리했고 `.collect()`를 쓰지 않아 "Spark 연산만 사용" 조건을 지켰습니다.
- 소문자 변환과 슬라이싱 전에 원래 토큰으로 길이를 필터링해서 순서가 올바릅니다.
- 문제 조건대로 문장부호를 제거하지 않았습니다 (예: `(b`, `“o`).

**개선 제안**
- `small_counts.map(lambda x: x[1]).sum()` → `small_counts.values().sum()`으로 더 간결하게 쓸 수 있습니다.
- `pairs` 단계를 앞의 `map`과 합칠 수 있습니다: `valid_words.map(lambda w: (w.lower()[:2], 1))`.
- `.take(5)` 확인용 셀은 개발 중에는 유용하지만, 최종본에서는 정리하거나 짧은 설명을 붙이면 좋습니다.

---

## Q2 — Integer pair statistics / 정수 쌍 통계

### English
**Result:** `2 (first) + 32 (max) + 0 (min) + 31 (count==1) + 28 (count==2) = 93`

**Good**
- The parsing is clear and the answer is correct.
- Each of the five values is in its own variable with a comment, so the code maps directly onto the question.

**Suggestions**
- **Fragile parsing:** `line.strip("()").split(", ")` only works if there is exactly one space after the comma. This version handles any spacing:
  `tuple(int(v) for v in line.strip("()").split(","))` (`int()` ignores surrounding spaces).
- **Blank lines:** an empty or trailing line would raise `ValueError`. Add `data.filter(lambda l: l.strip())` before parsing.
- **Repeated scans:** `pairs` is recomputed by 5 separate actions (`first`, `max`, `min`, two `count`s). Add `pairs.cache()`, or get both counts in one pass:
  `counts = first_elements.filter(lambda k: k in (1, 2)).countByValue()`.

### 한국어
**결과:** `2 (첫 원소) + 32 (최댓값) + 0 (최솟값) + 31 (1의 개수) + 28 (2의 개수) = 93`

**잘한 점**
- 파싱이 명확하고 답이 정확합니다.
- 다섯 값을 각각 변수로 나누고 주석을 달아서 문제와 코드가 1:1로 대응됩니다.

**개선 제안**
- **파싱 취약성:** `line.strip("()").split(", ")`는 쉼표 뒤에 공백이 정확히 한 칸일 때만 동작합니다. 아래처럼 쓰면 공백 개수와 상관없이 동작합니다:
  `tuple(int(v) for v in line.strip("()").split(","))` (`int()`는 앞뒤 공백을 무시합니다).
- **빈 줄 처리:** 빈 줄이나 마지막 개행이 있으면 `ValueError`가 납니다. 파싱 전에 `data.filter(lambda l: l.strip())`를 추가하세요.
- **반복 계산:** `pairs`가 5번의 액션(`first`, `max`, `min`, `count` ×2)마다 다시 계산됩니다. `pairs.cache()`를 쓰거나, 두 개수를 한 번에 구할 수 있습니다:
  `counts = first_elements.filter(lambda k: k in (1, 2)).countByValue()`.

---

## Q3 — World Happiness, non-decreasing scores / 행복지수 비감소 국가

### English
**Approach:** parse `(country, score)` per year, then chain-`join` all 5 years, flatten the nested tuples, and filter `s2015 <= … <= s2019`.

The overall approach is sound, but I checked against the raw data and found **three correctness issues**. The output has **28** countries. The correct answer is probably **32**.

**1. Floating-point noise drops Burundi** ❗
2017 scores are stored with float noise (e.g. `2.904999971`). Burundi's scores are `2.905, 2.905, 2.904999971, 2.905, 3.775`, so it is really "equal, equal, equal, up", but `2.905 <= 2.904999971` is `False`.
→ Round when parsing: `(x[0], round(float(x[2]), 3))`.

**2. Country names change between years** ❗
`join` silently drops any country whose name differs between years. Three renamed countries actually satisfy the condition:

| Name variants | Scores 2015→2019 |
|---|---|
| `Taiwan` / `Taiwan Province of China` | 6.298, 6.379, 6.422, 6.441, 6.446 |
| `Trinidad and Tobago` / `Trinidad & Tobago` | 6.168, 6.168, 6.168, 6.192, 6.192 |
| `Macedonia` / `North Macedonia` | 5.007, 5.121, 5.175, 5.185, 5.274 |

(Other variants are `Hong Kong S.A.R., China`, `Northern Cyprus`, and `Somaliland Region`, but those countries don't qualify.)
→ Normalize names with a small alias dict before the join. At minimum, mention this limitation in the notebook.

**3. `split(",")` breaks on quoted CSV fields** ⚠️
2017 contains `"Hong Kong S.A.R., China",71,5.472…`. A naive split gives the name `'"Hong Kong S.A.R.'` and reads the **rank (71)** as the score. It doesn't crash, which makes it worse: the value is silently wrong. It doesn't change this answer only because that row never joins.
→ Parse with the `csv` module: `rdd.map(lambda l: next(csv.reader([l])))`.

**Other suggestions**
- **Hard-coded column index:** cell 3 prints the headers, but `x[2]` is still hard-coded. Look the index up instead: `header.split(",").index("Happiness Score")`.
- **Readability:** `x[1][0][0][0][0]` is hard to read and easy to get wrong. An alternative that avoids nested tuples:
  ```python
  tagged = sc.union([scores[y].map(lambda x, y=y: (x[0], (y, x[1]))) for y in range(2015, 2020)])
  series = (tagged.groupByKey()
                  .filter(lambda kv: len(kv[1]) == 5)
                  .mapValues(lambda v: [s for _, s in sorted(v)]))
  result = series.filter(lambda kv: all(a <= b for a, b in zip(kv[1], kv[1][1:])))
  ```
- The `h=header` default-argument trick in the lambda is correct: it avoids the late-binding bug. 👍
- Cells 3 and 4 both read each file. Reuse the RDD, or `cache()` it.
- Small cleanup: `##Question` needs a space (`## Question`) to render as a heading. There is also an empty last cell.

### 한국어
**접근 방식:** 연도별로 `(국가, 점수)`를 추출하고, 5개 연도를 `join`으로 연결한 뒤 중첩 튜플을 펼쳐서 `s2015 <= … <= s2019` 조건으로 필터링했습니다.

전체 방향은 맞지만, 원본 데이터로 확인해 보니 **정확성 문제가 3가지** 있습니다. 출력은 **28개국**이지만 정답은 **32개국**일 가능성이 높습니다.

**1. 부동소수점 오차로 Burundi 누락** ❗
2017년 점수에 부동소수점 오차가 있습니다 (예: `2.904999971`). Burundi의 점수는 `2.905, 2.905, 2.904999971, 2.905, 3.775`로 실제로는 "같음, 같음, 같음, 상승"인데, `2.905 <= 2.904999971`이 `False`가 되어 빠집니다.
→ 파싱할 때 반올림하세요: `(x[0], round(float(x[2]), 3))`.

**2. 연도별 국가명 불일치** ❗
`join`은 이름이 다른 국가를 아무 경고 없이 제외합니다. 이름이 바뀐 국가 중 3개국이 실제로 조건을 만족합니다:

| 이름 표기 | 2015→2019 점수 |
|---|---|
| `Taiwan` / `Taiwan Province of China` | 6.298, 6.379, 6.422, 6.441, 6.446 |
| `Trinidad and Tobago` / `Trinidad & Tobago` | 6.168, 6.168, 6.168, 6.192, 6.192 |
| `Macedonia` / `North Macedonia` | 5.007, 5.121, 5.175, 5.185, 5.274 |

(그 밖에 `Hong Kong S.A.R., China`, `Northern Cyprus`, `Somaliland Region` 같은 표기 차이도 있지만, 이 국가들은 조건을 만족하지 않습니다.)
→ join 전에 별칭 딕셔너리로 국가명을 통일하세요. 최소한 노트북에 이 한계를 적어 두는 것이 좋습니다.

**3. `split(",")`가 따옴표로 묶인 CSV 필드를 잘못 나눔** ⚠️
2017년 파일에 `"Hong Kong S.A.R., China",71,5.472…` 행이 있습니다. 단순 split을 하면 국가명이 `'"Hong Kong S.A.R.'`가 되고 **순위(71)**를 점수로 읽습니다. 에러 없이 틀린 값이 들어가서 더 위험합니다. 이번 답에 영향이 없는 것은 이 행이 join에서 빠지기 때문일 뿐입니다.
→ `csv` 모듈로 파싱하세요: `rdd.map(lambda l: next(csv.reader([l])))`.

**기타 제안**
- **컬럼 인덱스 하드코딩:** 3번 셀에서 헤더를 출력했지만 `x[2]`는 여전히 하드코딩되어 있습니다. 인덱스를 찾아서 쓰세요: `header.split(",").index("Happiness Score")`.
- **가독성:** `x[1][0][0][0][0]`은 읽기 어렵고 실수하기 쉽습니다. 중첩 튜플 없이 쓰는 방법입니다 (위 English 섹션의 `union` + `groupByKey` 코드 참고).
- lambda에서 `h=header` 기본 인자를 쓴 것은 늦은 바인딩(late binding) 버그를 피하는 올바른 방법입니다. 👍
- 3번과 4번 셀에서 같은 파일을 두 번 읽습니다. RDD를 재사용하거나 `cache()`를 쓰세요.
- 사소한 정리: `##Question`은 띄어쓰기를 넣어야 (`## Question`) 제목으로 표시됩니다. 마지막 빈 셀도 있습니다.
