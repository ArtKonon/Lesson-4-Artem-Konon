package workerpool

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestRunPool(t *testing.T) {
	// 1. Успішне виконання завдань
	t.Run("Success", func(t *testing.T) {
		t.Parallel()
		jobs := make(chan Job, 2)

		jobs <- Job{
			ID: "job-1",
			Fetch: func(ctx context.Context) (int, error) {
				return 100, nil
			},
		}
		jobs <- Job{
			ID: "job-2",
			Fetch: func(ctx context.Context) (int, error) {
				return 200, nil
			},
		}
		close(jobs)

		results := RunPool(jobs, 2, time.Second)

		var count int
		for res := range results {
			count++
			if res.Err != nil {
				t.Errorf("Очікувалося успішне виконання, отримано помилку: %v", res.Err)
			}
			if res.Size != 100 && res.Size != 200 {
				t.Errorf("Несподіваний розмір даних (Size): %d", res.Size)
			}
		}

		if count != 2 {
			t.Errorf("Очікувалося 2 результати, отримано: %d", count)
		}
	})

	// 2. Ранній вихід: 0 (або менше) воркерів
	t.Run("ZeroWorkers", func(t *testing.T) {
		t.Parallel()
		jobs := make(chan Job, 1)
		jobs <- Job{
			ID: "job-ignored",
			Fetch: func(ctx context.Context) (int, error) {
				return 1, nil
			},
		}
		close(jobs)

		// Передаємо 0 воркерів
		results := RunPool(jobs, 0, time.Second)

		// Канал має бути закритий негайно, ітерація не виконається
		var count int
		for range results {
			count++
		}

		if count != 0 {
			t.Errorf("Очікувалося 0 результатів (порожній канал), отримано: %d", count)
		}
	})

	// 3. Скасування за таймаутом контексту
	t.Run("TimeoutContext", func(t *testing.T) {
		t.Parallel()
		jobs := make(chan Job, 1)
		jobs <- Job{
			ID: "job-timeout",
			Fetch: func(ctx context.Context) (int, error) {
				select {
				case <-ctx.Done(): // Очікуємо, що контекст завершиться за таймаутом
					return 0, ctx.Err()
				case <-time.After(500 * time.Millisecond):
					return 10, nil
				}
			},
		}
		close(jobs)

		// Передаємо дуже короткий таймаут
		timeout := 10 * time.Millisecond
		results := RunPool(jobs, 1, timeout)

		res := <-results

		if res.JobID != "job-timeout" {
			t.Errorf("Очікувався ID 'job-timeout', отримано: %s", res.JobID)
		}
		if !errors.Is(res.Err, context.DeadlineExceeded) {
			t.Errorf("Очікувалася помилка DeadlineExceeded, отримано: %v", res.Err)
		}
	})

	// 4. Обробка помилки, яка повертається з Fetch
	t.Run("FetchError", func(t *testing.T) {
		t.Parallel()
		expectedErr := errors.New("custom fetch error")
		jobs := make(chan Job, 1)
		jobs <- Job{
			ID: "job-error",
			Fetch: func(ctx context.Context) (int, error) {
				return 0, expectedErr
			},
		}
		close(jobs)

		results := RunPool(jobs, 1, time.Second)

		res := <-results

		if res.Err != expectedErr {
			t.Errorf("Очікувалася помилка '%v', отримано: '%v'", expectedErr, res.Err)
		}
		if res.Size != 0 {
			t.Errorf("Очікувався нульовий Size при помилці, отримано: %d", res.Size)
		}
	})

	// 5. Конкурентне виконання (кількість завдань більша за кількість воркерів)
	t.Run("ConcurrencyAndLoad", func(t *testing.T) {
		t.Parallel()
		const numJobs = 15
		const numWorkers = 3

		jobs := make(chan Job, numJobs)

		for i := 0; i < numJobs; i++ {
			jobs <- Job{
				ID: strconv.Itoa(i),
				Fetch: func(ctx context.Context) (int, error) {
					// Імітуємо невелику затримку
					time.Sleep(10 * time.Millisecond)
					return 1, nil
				},
			}
		}
		close(jobs)

		results := RunPool(jobs, numWorkers, time.Second)

		var count int
		for res := range results {
			count++
			if res.Err != nil {
				t.Errorf("Очікувалося виконання без помилок, але у завдання %s помилка: %v", res.JobID, res.Err)
			}
		}

		if count != numJobs {
			t.Errorf("Очікувалося результатів: %d, отримано: %d", numJobs, count)
		}
	})
}
