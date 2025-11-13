from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
from datetime import datetime, timedelta

def parse_ranking_table(html_content, current_date):
    """
    HTML 콘텐츠에서 팀 순위 테이블을 BeautifulSoup으로 직접 파싱하여 데이터프레임을 반환합니다.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 랭킹 테이블 찾기
    ranking_table = soup.find('table', 
                              {'summary': '순위, 팀명,승,패,무,승률,승차,최근10경기,연속,홈,방문', 
                               'class': 'tData'})
    
    if not ranking_table:
        return None

    # 헤더 추출
    header_row = ranking_table.find('thead').find('tr')
    headers = [th.text.strip() for th in header_row.find_all('th')]
    headers.append('기준일') # 기준일 컬럼 추가

    # 데이터 추출
    data_rows = ranking_table.find('tbody').find_all('tr')
    team_rank_data = []

    for row in data_rows:
        cols = row.find_all('td')
        row_data = [col.text.strip() for col in cols]
        
        if row_data:
            row_data.append(current_date)
            team_rank_data.append(row_data)
        
    if team_rank_data:
        # 데이터프레임 생성 시, 컬럼 수가 맞는지 확인
        if len(headers) == len(team_rank_data[0]):
            return pd.DataFrame(team_rank_data, columns=headers)
    return None

def scrape_kbo_daily_rank_by_date_range(start_date: str, end_date: str):
    """
    지정된 날짜 범위 (YYYYMMDD)의 KBO 일자별 팀 순위를 크롤링하고 지정된 경로에 CSV로 저장합니다.
    """
    URL = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
    all_season_data = []
    
    # 입력 날짜 유효성 검사 및 YYYYMMDD 형식으로 변환
    try:
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        target_year = start_dt.strftime('%Y')
        
        # 시작 날짜가 종료 날짜보다 늦을 경우
        if start_dt > end_dt:
            print("❌ 오류: 시작 날짜가 종료 날짜보다 늦습니다.")
            return
            
    except ValueError:
        print("❌ 오류: 날짜 형식이 올바르지 않습니다. YYYYMMDD 형식(예: 20210403)으로 입력해주세요.")
        return

    driver = webdriver.Chrome() 

    try:
        driver.get(URL)
        print(f"웹페이지 로드 완료: {URL}")

        wait = WebDriverWait(driver, 15)
        
        # --- 1. 시즌 시작일로 이동 (클릭 기반) ---
        target_date_text = start_dt.strftime('%Y.%m.%d')
        
        # 주요 요소 ID/XPath
        date_id = "cphContents_cphContents_cphContents_lblSearchDateTitle"
        search_date_hf_id = "cphContents_cphContents_cphContents_hfSearchDate"
        table_xpath = "//table[@summary='순위, 팀명,승,패,무,승률,승차,최근10경기,연속,홈,방문']"
        calendar_icon_css = ".ui-datepicker-trigger"
        next_date_btn_id = "cphContents_cphContents_cphContents_btnNextDate"


        # 1. 캘린더 아이콘 클릭 (위젯 열기)
        calendar_icon = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, calendar_icon_css)))
        calendar_icon.click()
        
        # 2. 연도 선택 (Select 클래스 사용)
        year_select_element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "ui-datepicker-year")))
        Select(year_select_element).select_by_value(target_year)
        
        # 3. 월 선택 (Select 클래스 사용)
        month_select_element = driver.find_element(By.CLASS_NAME, "ui-datepicker-month")
        # datetime 객체에서 0부터 시작하는 월 인덱스를 가져옵니다 (0: 1월, 3: 4월)
        month_value = str(start_dt.month - 1)
        Select(month_select_element).select_by_value(month_value) 
        
        # 4. 날짜 클릭
        target_day = start_dt.strftime('%#d') # 0 없는 일자 형식
        date_xpath = f"//div[@id='ui-datepicker-div']//a[@class='ui-state-default' and text()='{target_day}']"
        target_day_element = wait.until(EC.element_to_be_clickable((By.XPATH, date_xpath)))
        target_day_element.click()
        
        # 5. AJAX 로딩 대기
        wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))
        wait.until(EC.text_to_be_present_in_element((By.ID, date_id), target_date_text))

        print(f"지정된 시작일({start_date})로 이동 완료.")

        # --- 2. '다음 날짜' 버튼을 이용한 순방향 반복 크롤링 ---
        
        current_dt = start_dt
        while current_dt <= end_dt:
            try:
                # 1. 현재 날짜 정보 확인 (루프 시작 시 current_dt는 이미 설정되어 있음)
                date_element = driver.find_element(By.ID, date_id)
                current_date_text = date_element.text
                current_date_value = driver.find_element(By.ID, search_date_hf_id).get_attribute('value').strip()
                
                # Hidden Field 값과 현재 반복 날짜 일치 확인
                if current_date_value != current_dt.strftime('%Y%m%d'):
                    # 날짜가 예상과 다르면 건너뜁니다. (경기 없는 날짜는 다음 경기일로 건너뜁니다.)
                    current_dt = datetime.strptime(current_date_value, '%Y%m%d')
                    print(f"[정보] 데이터 없음, 다음 경기일({current_date_value})로 자동 이동됨.")

                # 2. 크롤링 범위 초과 확인 및 종료
                if current_dt > end_dt:
                    print(f"\n[알림] 크롤링 종료 날짜({end_date})를 초과했습니다. 크롤링을 종료합니다.")
                    break
                    
                print(f"[크롤링 중] 기준일: {current_date_text} ({current_date_value})")
                
                # 3. 순위표 추출 및 데이터 저장
                html = driver.page_source
                current_rank_df = parse_ranking_table(html, current_date_text)
                
                if current_rank_df is not None:
                    all_season_data.append(current_rank_df)
                    print(f"-> {len(current_rank_df)}개 팀 순위 데이터 수집 완료.")
                else:
                    # 데이터는 없지만 날짜는 있으므로 루프를 계속 진행
                    print("-> 순위표 데이터를 찾지 못했습니다 (경기 미개최일일 수 있음).")
                
                # 4. 다음 날짜 버튼 클릭
                next_date_button = driver.find_element(By.ID, next_date_btn_id)
                date_element_before_click = driver.find_element(By.ID, date_id)

                next_date_button.click()
                
                # 5. AJAX 로딩 대기 및 날짜 업데이트
                wait.until(EC.staleness_of(date_element_before_click))
                wait.until(EC.presence_of_element_located((By.ID, date_id)))
                
                # 다음 반복을 위해 날짜를 1일 증가시킵니다.
                current_dt = current_dt + timedelta(days=1)
                
            except TimeoutException:
                print("\n[오류] 페이지 로드 중 시간 초과가 발생했습니다. 루프를 종료합니다.")
                break
            except Exception as e:
                print(f"\n[예외 발생] 알 수 없는 오류: {e}")
                break
                
    finally:
        driver.quit()
        print("\nSelenium 드라이버 종료.")
        
        # --- 3. 최종 데이터 통합 및 CSV 저장 ---
        if all_season_data:
            final_df = pd.concat(all_season_data).reset_index(drop=True)
            
            # 저장 경로 및 파일명 설정
            output_dir = f"./data/{target_year}/league_info/"
            output_filename = f"{target_year}_team_rank_daily.csv"
            
            # 폴더 생성
            os.makedirs(output_dir, exist_ok=True)
            
            output_path = os.path.join(output_dir, output_filename)
            
            # CSV 파일 저장 (한글 깨짐 방지를 위해 encoding='utf-8-sig' 사용)
            final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            print("\n\n=============== 최종 크롤링 결과 ===============\n")
            print(f"총 {len(final_df['기준일'].unique())}일의 데이터 ({len(final_df)}개 행) 수집 완료.")
            print(f"\n데이터가 **'{output_path}'** 파일로 저장되었습니다.")
        else:
            print("크롤링된 데이터가 없습니다.")

# --- 사용 예시 ---
if __name__ == '__main__':
    start = "20250322"
    end = "20251004" 

    # 함수를 호출하여 크롤링 실행
    scrape_kbo_daily_rank_by_date_range(start_date=start, end_date=end)