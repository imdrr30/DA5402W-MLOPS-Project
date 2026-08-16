import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, RouterModule } from '@angular/router';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-login',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterModule],
  templateUrl: './login.html'
})
export class LoginComponent {
  email = '';
  password = '';
  errorMsg = signal<string>('');
  loading = signal<boolean>(false);

  constructor(
    private readonly apiService: ApiService,
    private readonly router: Router
  ) {}

  onSubmit() {
    if (!this.email || !this.password) {
      this.errorMsg.set('Please fill in all fields.');
      return;
    }

    this.loading.set(true);
    this.errorMsg.set('');

    this.apiService.login({ email: this.email, password: this.password }).subscribe({
      next: (res) => {
        this.loading.set(false);
        this.router.navigate(['/']);
      },
      error: (err) => {
        this.loading.set(false);
        this.errorMsg.set(err.error?.error || 'Invalid credentials. Please try again.');
      }
    });
  }
}
