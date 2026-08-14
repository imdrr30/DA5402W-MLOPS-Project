import { Component, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, RouterModule } from '@angular/router';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-register',
  standalone: true,
  imports: [CommonModule, FormsModule, RouterModule],
  templateUrl: './register.html'
})
export class RegisterComponent {
  first_name = '';
  last_name = '';
  email = '';
  phone_number = '';
  region = 'North America';
  country = 'USA';
  password = '';
  
  errorMsg = signal<string>('');
  successMsg = signal<string>('');
  loading = signal<boolean>(false);

  constructor(
    private readonly apiService: ApiService,
    private readonly router: Router
  ) {}

  onSubmit() {
    if (!this.email || !this.password || !this.first_name || !this.last_name) {
      this.errorMsg.set('Please fill in all required fields.');
      return;
    }

    this.loading.set(true);
    this.errorMsg.set('');
    this.successMsg.set('');

    const userData = {
      first_name: this.first_name,
      last_name: this.last_name,
      email: this.email,
      phone_number: this.phone_number,
      region: this.region,
      country: this.country,
      password: this.password
    };

    this.apiService.register(userData).subscribe({
      next: (res) => {
        this.loading.set(false);
        this.successMsg.set('Registration successful! Redirecting to login page...');
        setTimeout(() => {
          this.router.navigate(['/login']);
        }, 1500);
      },
      error: (err) => {
        this.loading.set(false);
        this.errorMsg.set(err.error?.error || 'Registration failed. Email might already be taken.');
      }
    });
  }
}
